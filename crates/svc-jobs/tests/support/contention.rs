use std::time::Duration;

use bc_jobs::commands::fleet::{ObservePresence, observe_presence};
use bc_jobs::commands::job::create::{CreateJob, CreateOutcome, create_job};
use bc_jobs::commands::job::dispatch::DispatchRun;
use bc_jobs::domain::attempts::MaxAttempts;
use bc_jobs::domain::fleet::RunnerType;
use bc_jobs::domain::fleet::capacity::Capacity;
use bc_jobs::domain::fleet::status::ReportedStatus;
use bc_jobs::domain::ids::{JobId, PresenceSessionId, RunId, RunnerTypeId};
use bc_jobs::domain::job::Job;
use bc_jobs::domain::keys::{InstanceKey, ProducerKey, RunnerTypeKey, RunnerVersion};
use bc_jobs::domain::policy::ServiceLimits;
use bc_jobs::ports::environment::IdFactory;
use bc_jobs::ports::fleet::FleetReader;
use bc_jobs::ports::job::JobReader;
use br_core_events::{Actor, EventMetadata, ServiceAccountId};
use br_test_harness::E2eDatabase;
use chrono::TimeDelta;
use sqlx::{PgPool, Row};
use svc_jobs::ServiceError;
use svc_jobs::app::environment::UuidV7Factory;
use svc_jobs::app::write::{self, FleetChange, JobChange};
use svc_jobs::db::PgStore;
use uuid::Uuid;

use super::clock;
use super::fixture::require_provisioned_infrastructure;

pub struct Fixture {
    db: Option<E2eDatabase>,
    pub store: PgStore,
    pub pool: PgPool,
}

impl Fixture {
    pub async fn start() -> Self {
        require_provisioned_infrastructure();
        let db = E2eDatabase::create(true, &[]).await;
        let pool = PgPool::connect(&db.owner_migration_url())
            .await
            .expect("the owner connection opens");
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("the declared schema migrates");
        Self {
            db: Some(db),
            store: PgStore::new(pool.clone()),
            pool,
        }
    }

    pub async fn count(&self, sql: &str, binding: &str) -> i64 {
        sqlx::query(sql)
            .bind(binding)
            .fetch_one(&self.pool)
            .await
            .expect("the count query runs")
            .get("count")
    }

    pub async fn fleet_event_version(&self, event_type: &str) -> i64 {
        sqlx::query_scalar(
            "SELECT aggregate_version FROM domain_events WHERE event_type = $1 \
             ORDER BY aggregate_version DESC LIMIT 1",
        )
        .bind(event_type)
        .fetch_one(&self.pool)
        .await
        .unwrap_or_else(|error| panic!("no {event_type} was ever recorded: {error}"))
    }

    pub async fn await_a_pod_blocked_on_the_fleet_lock(&self) {
        const PROBES: u32 = 100;
        for _ in 0..PROBES {
            if self.pods_blocked_holding_the_runner_type_lock().await > 0 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!(
            "no backend ever blocked while holding a lock on the runner type relation, so the \
             interleaving this scenario needs never happened and it would prove nothing",
        );
    }

    async fn pods_blocked_holding_the_runner_type_lock(&self) -> i64 {
        sqlx::query_scalar(
            "SELECT count(*) FROM pg_locks waiting \
             JOIN pg_locks held ON held.pid = waiting.pid AND held.granted \
             JOIN pg_class relation ON relation.oid = held.relation \
             JOIN pg_stat_activity backend ON backend.pid = waiting.pid \
             WHERE NOT waiting.granted AND relation.relname = 'runner_types' \
               AND backend.datname = current_database()",
        )
        .fetch_one(&self.pool)
        .await
        .expect("the lock-wait probe runs")
    }

    pub async fn shutdown(mut self) {
        self.pool.close().await;
        if let Some(db) = self.db.take() {
            db.cleanup().await;
        }
    }
}

pub fn limits() -> ServiceLimits {
    ServiceLimits::new(
        MaxAttempts::new(3).expect("a positive ceiling"),
        MaxAttempts::new(3).expect("a positive default"),
        TimeDelta::seconds(600),
        TimeDelta::seconds(600),
    )
    .expect("coherent limits")
}

pub fn metadata() -> EventMetadata {
    EventMetadata::new(
        Actor::Service(ServiceAccountId::from(svc_jobs::app::SERVICE_ACTOR)),
        Uuid::now_v7(),
    )
}

pub fn one_run() -> Capacity {
    Capacity::new(1).expect("a positive capacity")
}

pub fn ids() -> UuidV7Factory {
    UuidV7Factory
}

pub async fn commit(fixture: &Fixture, changes: Vec<JobChange>) -> Result<(), ServiceError> {
    write::commit_job_changes(&fixture.store, &ids(), changes, &metadata(), clock::now()).await
}

pub async fn a_job_with_one_dispatched_run(fixture: &Fixture, runner_type: &str) -> (Job, RunId) {
    let job_id = JobId::new(Uuid::now_v7()).expect("a v7 job id");
    let declaration = CreateJob {
        id: job_id,
        runner_type: RunnerTypeKey::new(runner_type).expect("a valid runner type"),
        producer: ProducerKey::new("projects").expect("a valid producer"),
        config: None,
        parent_job_id: None,
        triggered_by: None,
        source_entity_id: None,
        max_attempts: None,
    };
    let CreateOutcome::Queued(queued) =
        create_job(declaration, None, None, None, &limits()).expect("the declaration is accepted")
    else {
        panic!("a fresh job id is queued, never already queued");
    };
    commit(fixture, vec![JobChange::new(job_id, None, queued.events)])
        .await
        .expect("the job is queued");

    let job = JobReader::load(&fixture.store, job_id)
        .await
        .expect("the queued job loads")
        .expect("the queued job exists");
    let run_id = RunId::new(Uuid::now_v7()).expect("a v7 run id");
    let dispatched = job
        .dispatch_run(
            DispatchRun {
                run_id,
                at: clock::now(),
            },
            &limits(),
        )
        .expect("the first run dispatches");
    commit(
        fixture,
        vec![JobChange::new(job_id, Some(job), dispatched.events)],
    )
    .await
    .expect("the run is dispatched");

    let job = JobReader::load(&fixture.store, job_id)
        .await
        .expect("the dispatched job loads")
        .expect("the dispatched job exists");
    (job, run_id)
}

pub async fn a_registered_instance(fixture: &Fixture, runner_type: &str) -> RunnerType {
    let key = RunnerTypeKey::new(runner_type).expect("a valid runner type");
    let runner_type_id = RunnerTypeId::new(ids().next()).expect("a v7 runner type id");
    let connected = observe_presence(None, announcing(runner_type_id, &key, "instance-a"))
        .expect("a first presence registers the type and the instance");
    write::commit_fleet_events(
        &fixture.store,
        &ids(),
        FleetChange {
            runner_type_id,
            runner_type: &key,
            decided_on: None,
            events: &connected.events,
        },
        &metadata(),
        clock::now(),
    )
    .await
    .expect("the instance connects");
    reloaded(fixture, &key).await
}

pub async fn a_second_live_instance(
    fixture: &Fixture,
    fleet: &RunnerType,
    instance_key: &str,
) -> RunnerType {
    let connected = observe_presence(
        Some(fleet),
        announcing(fleet.id(), fleet.key(), instance_key),
    )
    .expect("a new instance of a known type connects");
    write::commit_fleet_events(
        &fixture.store,
        &ids(),
        FleetChange {
            runner_type_id: fleet.id(),
            runner_type: fleet.key(),
            decided_on: Some(fleet),
            events: &connected.events,
        },
        &metadata(),
        clock::now(),
    )
    .await
    .expect("the second instance connects");
    reloaded(fixture, fleet.key()).await
}

pub fn announcing(
    runner_type_id: RunnerTypeId,
    runner_type: &RunnerTypeKey,
    instance_key: &str,
) -> ObservePresence {
    ObservePresence {
        runner_type_id,
        runner_type: runner_type.clone(),
        instance_key: InstanceKey::new(instance_key).expect("a valid instance key"),
        session_id: PresenceSessionId::new(ids().next()).expect("a v7 session id"),
        version: RunnerVersion::new("1.4.2").expect("a valid version"),
        reported_status: ReportedStatus::Ready,
        capacity: one_run(),
    }
}

pub async fn reloaded(fixture: &Fixture, key: &RunnerTypeKey) -> RunnerType {
    FleetReader::load(&fixture.store, key)
        .await
        .expect("the fleet loads")
        .expect("the runner type is live")
}
