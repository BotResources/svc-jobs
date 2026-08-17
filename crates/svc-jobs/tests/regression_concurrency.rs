mod support;

use std::time::Duration;

use bc_jobs::commands::fleet::{ObserveLoss, ObservePresence, observe_loss, observe_presence};
use bc_jobs::commands::job::create::{CreateJob, CreateOutcome, create_job};
use bc_jobs::commands::job::dispatch::DispatchRun;
use bc_jobs::commands::job::run_outcome::RunCompletedFact;
use bc_jobs::commands::job::run_progress::RunStartedFact;
use bc_jobs::domain::attempts::MaxAttempts;
use bc_jobs::domain::fleet::RunnerType;
use bc_jobs::domain::fleet::capacity::Capacity;
use bc_jobs::domain::fleet::status::ReportedStatus;
use bc_jobs::domain::ids::{EventId, JobId, PresenceSessionId, RunId, RunnerTypeId};
use bc_jobs::domain::job::Job;
use bc_jobs::domain::keys::{InstanceKey, ProducerKey, ReasonCode, RunnerTypeKey, RunnerVersion};
use bc_jobs::domain::policy::ServiceLimits;
use bc_jobs::domain::run::parts::RunnerInstanceReference;
use bc_jobs::event::fleet::FleetEvent;
use bc_jobs::ports::environment::IdFactory;
use bc_jobs::ports::fleet::FleetReader;
use bc_jobs::ports::job::JobReader;
use br_core_events::{Actor, EventMetadata, ServiceAccountId};
use br_test_harness::E2eDatabase;
use chrono::{TimeDelta, Utc};
use sqlx::{PgPool, Row};
use svc_jobs::ServiceError;
use svc_jobs::app::environment::{SystemClock, UuidV7Factory};
use svc_jobs::app::presence;
use svc_jobs::app::write::{self, FleetChange, JobChange};
use svc_jobs::db::{PgStore, apply};
use uuid::Uuid;

use support::fixture::require_provisioned_infrastructure;

struct Fixture {
    db: Option<E2eDatabase>,
    store: PgStore,
    pool: PgPool,
}

impl Fixture {
    async fn start() -> Self {
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

    async fn count(&self, sql: &str, binding: &str) -> i64 {
        sqlx::query(sql)
            .bind(binding)
            .fetch_one(&self.pool)
            .await
            .expect("the count query runs")
            .get("count")
    }

    async fn await_a_pod_blocked_on_the_fleet_lock(&self) {
        const PROBES: u32 = 100;
        for _ in 0..PROBES {
            let waiting: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM pg_stat_activity \
                 WHERE datname = current_database() AND wait_event_type = 'Lock'",
            )
            .fetch_one(&self.pool)
            .await
            .expect("the lock-wait probe runs");
            if waiting > 0 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!(
            "no backend ever blocked on the runner type row, so the interleaving this scenario \
             needs never happened and it would prove nothing",
        );
    }

    async fn shutdown(mut self) {
        self.pool.close().await;
        if let Some(db) = self.db.take() {
            db.cleanup().await;
        }
    }
}

fn limits() -> ServiceLimits {
    ServiceLimits::new(
        MaxAttempts::new(3).expect("a positive ceiling"),
        MaxAttempts::new(3).expect("a positive default"),
        TimeDelta::seconds(600),
        TimeDelta::seconds(600),
    )
    .expect("coherent limits")
}

fn metadata() -> EventMetadata {
    EventMetadata::new(
        Actor::Service(ServiceAccountId::from(svc_jobs::app::SERVICE_ACTOR)),
        Uuid::now_v7(),
    )
}

fn one_run() -> Capacity {
    Capacity::new(1).expect("a positive capacity")
}

fn ids() -> UuidV7Factory {
    UuidV7Factory
}

async fn commit(fixture: &Fixture, changes: Vec<JobChange>) -> Result<(), ServiceError> {
    write::commit_job_changes(&fixture.store, &ids(), changes, &metadata(), Utc::now()).await
}

async fn a_job_with_one_dispatched_run(fixture: &Fixture, runner_type: &str) -> (Job, RunId) {
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
                at: Utc::now(),
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

#[tokio::test]
async fn two_instances_replaying_the_same_run_start_write_one_fact_and_publish_it_once() {
    // Given: a job with one dispatched run, and the state both instances hydrated before deciding
    let fixture = Fixture::start().await;
    let runner_type = "analyst";
    let (decided_on, run_id) = a_job_with_one_dispatched_run(&fixture, runner_type).await;
    let fact = RunStartedFact {
        run_id,
        instance: RunnerInstanceReference::new(
            RunnerTypeKey::new(runner_type).expect("a valid runner type"),
            InstanceKey::new("instance-a").expect("a valid instance key"),
        ),
    };

    // When: both instances decide RunStarted on that same state and commit at the same time
    let first = decided_on
        .record_run_started(fact.clone())
        .expect("the first instance decides the fact");
    let second = decided_on
        .record_run_started(fact)
        .expect("the second instance decides the same fact");
    assert_eq!(first.events.len(), 1);
    assert_eq!(second.events.len(), 1);
    let job_id = decided_on.id();
    let (left, right) = tokio::join!(
        commit(
            &fixture,
            vec![JobChange::new(
                job_id,
                Some(decided_on.clone()),
                first.events
            )]
        ),
        commit(
            &fixture,
            vec![JobChange::new(job_id, Some(decided_on), second.events)]
        ),
    );

    // Then: exactly one of them wrote, the other is told the state moved under it
    let outcomes = [left, right];
    let refused: Vec<&ServiceError> = outcomes
        .iter()
        .filter_map(|outcome| outcome.as_ref().err())
        .collect();
    assert_eq!(
        refused.len(),
        1,
        "exactly one duplicate must be refused, got {outcomes:?}",
    );
    assert!(
        matches!(refused[0], ServiceError::Contended),
        "the loser learns the state moved, not an infrastructure failure: {:?}",
        refused[0],
    );

    // Then: the history holds one RunStarted and the bus is offered one job.started.v1
    assert_eq!(
        fixture
            .count(
                "SELECT count(*) AS count FROM domain_events WHERE event_type = $1",
                "RunStarted",
            )
            .await,
        1,
        "a duplicated runner fact never doubles the domain history",
    );
    assert_eq!(
        fixture
            .count(
                "SELECT count(*) AS count FROM integration_outbox WHERE subject = $1",
                "integration.evt.jobs.job.started.v1",
            )
            .await,
        1,
        "a duplicated runner fact never publishes the integration event twice",
    );
    fixture.shutdown().await;
}

async fn a_registered_instance(fixture: &Fixture, runner_type: &str) -> RunnerType {
    let key = RunnerTypeKey::new(runner_type).expect("a valid runner type");
    let runner_type_id = RunnerTypeId::new(
        fixture
            .store
            .ensure_runner_type(&key)
            .await
            .expect("the runner type row exists"),
    )
    .expect("a v7 runner type id");
    let connected = observe_presence(
        None,
        ObservePresence {
            runner_type_id,
            runner_type: key.clone(),
            instance_key: InstanceKey::new("instance-a").expect("a valid instance key"),
            session_id: PresenceSessionId::new(ids().next()).expect("a v7 session id"),
            version: RunnerVersion::new("1.4.2").expect("a valid version"),
            reported_status: ReportedStatus::Ready,
            capacity: one_run(),
        },
    )
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
        Utc::now(),
    )
    .await
    .expect("the instance connects");
    FleetReader::load(&fixture.store, &key)
        .await
        .expect("the fleet loads")
        .expect("the runner type is live")
}

#[tokio::test]
async fn two_instances_replaying_the_same_presence_change_write_one_fleet_fact() {
    // Given: a live runner instance, and the fleet state both instances hydrated before deciding
    let fixture = Fixture::start().await;
    let runner_type = "reporter";
    let key = RunnerTypeKey::new(runner_type).expect("a valid runner type");
    let decided_on = a_registered_instance(&fixture, runner_type).await;
    let reported = |status: ReportedStatus| ObservePresence {
        runner_type_id: decided_on.id(),
        runner_type: key.clone(),
        instance_key: InstanceKey::new("instance-a").expect("a valid instance key"),
        session_id: PresenceSessionId::new(ids().next()).expect("a v7 session id"),
        version: RunnerVersion::new("1.4.2").expect("a valid version"),
        reported_status: status,
        capacity: one_run(),
    };

    // When: the KV watch hands the same status change to both instances at once
    let first = observe_presence(Some(&decided_on), reported(ReportedStatus::Draining))
        .expect("the first instance decides the change");
    let second = observe_presence(Some(&decided_on), reported(ReportedStatus::Draining))
        .expect("the second instance decides the same change");
    assert!(matches!(
        first.events.as_slice(),
        [FleetEvent::InstanceStatusReported(_)]
    ));
    let left_metadata = metadata();
    let right_metadata = metadata();
    let factory = ids();
    let (left, right) = tokio::join!(
        write::commit_fleet_events(
            &fixture.store,
            &factory,
            FleetChange {
                runner_type_id: decided_on.id(),
                runner_type: &key,
                decided_on: Some(&decided_on),
                events: &first.events,
            },
            &left_metadata,
            Utc::now(),
        ),
        write::commit_fleet_events(
            &fixture.store,
            &factory,
            FleetChange {
                runner_type_id: decided_on.id(),
                runner_type: &key,
                decided_on: Some(&decided_on),
                events: &second.events,
            },
            &right_metadata,
            Utc::now(),
        ),
    );

    // Then: exactly one instance recorded the change — the other reclaims nothing
    let outcomes = [left, right];
    let refused: Vec<&ServiceError> = outcomes
        .iter()
        .filter_map(|outcome| outcome.as_ref().err())
        .collect();
    assert_eq!(
        refused.len(),
        1,
        "a broadcast presence change must be recorded by one instance only, got {outcomes:?}",
    );
    assert!(
        matches!(refused[0], ServiceError::Contended),
        "the loser learns the fleet moved under it: {:?}",
        refused[0],
    );
    assert_eq!(
        fixture
            .count(
                "SELECT count(*) AS count FROM domain_events WHERE event_type = $1",
                "InstanceStatusReported",
            )
            .await,
        1,
        "a broadcast presence change never doubles the fleet history",
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn only_one_instance_records_a_presence_loss_so_orphaned_runs_are_reclaimed_once() {
    // Given: a live runner instance, seen by both service instances before either reacts
    let fixture = Fixture::start().await;
    let runner_type = "collector";
    let key = RunnerTypeKey::new(runner_type).expect("a valid runner type");
    let decided_on = a_registered_instance(&fixture, runner_type).await;
    let instance_key = InstanceKey::new("instance-a").expect("a valid instance key");
    let loss = || ObserveLoss {
        instance_key: instance_key.clone(),
        reason_code: ReasonCode::new(bc_jobs::commands::fleet::PRESENCE_EXPIRED)
            .expect("a valid reason code"),
    };

    // When: the KV watch hands the same expiry to both instances at once
    let first = observe_loss(&decided_on, loss()).expect("the first instance decides the loss");
    let second = observe_loss(&decided_on, loss()).expect("the second instance decides the loss");
    let left_metadata = metadata();
    let right_metadata = metadata();
    let factory = ids();
    let (left, right) = tokio::join!(
        write::commit_fleet_events(
            &fixture.store,
            &factory,
            FleetChange {
                runner_type_id: decided_on.id(),
                runner_type: &key,
                decided_on: Some(&decided_on),
                events: &first.events,
            },
            &left_metadata,
            Utc::now(),
        ),
        write::commit_fleet_events(
            &fixture.store,
            &factory,
            FleetChange {
                runner_type_id: decided_on.id(),
                runner_type: &key,
                decided_on: Some(&decided_on),
                events: &second.events,
            },
            &right_metadata,
            Utc::now(),
        ),
    );

    // Then: only one instance holds the loss, so run reclamation runs once and not twice
    let outcomes = [left, right];
    let refused: Vec<&ServiceError> = outcomes
        .iter()
        .filter_map(|outcome| outcome.as_ref().err())
        .collect();
    assert_eq!(
        refused.len(),
        1,
        "a disconnection carries no unique key of its own, so only the guard can settle the \
         race: {outcomes:?}",
    );
    assert!(matches!(refused[0], ServiceError::Contended));
    assert_eq!(
        fixture
            .count(
                "SELECT count(*) AS count FROM domain_events WHERE event_type = $1",
                "InstanceDisconnected",
            )
            .await,
        1,
    );
    fixture.shutdown().await;
}

async fn a_second_live_instance(
    fixture: &Fixture,
    fleet: &RunnerType,
    instance_key: &str,
) -> RunnerType {
    let connected = observe_presence(
        Some(fleet),
        ObservePresence {
            runner_type_id: fleet.id(),
            runner_type: fleet.key().clone(),
            instance_key: InstanceKey::new(instance_key).expect("a valid instance key"),
            session_id: PresenceSessionId::new(ids().next()).expect("a v7 session id"),
            version: RunnerVersion::new("1.4.2").expect("a valid version"),
            reported_status: ReportedStatus::Ready,
            capacity: one_run(),
        },
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
        Utc::now(),
    )
    .await
    .expect("the second instance connects");
    FleetReader::load(&fixture.store, fleet.key())
        .await
        .expect("the fleet loads")
        .expect("the runner type is live")
}

#[tokio::test]
async fn an_instance_loss_is_recorded_even_when_the_rest_of_the_fleet_moves_under_it() {
    // Given: two live instances of one runner type
    let fixture = Fixture::start().await;
    let runner_type = "haulier";
    let key = RunnerTypeKey::new(runner_type).expect("a valid runner type");
    let first = a_registered_instance(&fixture, runner_type).await;
    let fleet = a_second_live_instance(&fixture, &first, "instance-b").await;
    let instance_b = InstanceKey::new("instance-b").expect("a valid instance key");

    // Given: another pod holds the runner type, about to report a status change of instance-a
    let mut concurrent = fixture
        .pool
        .begin()
        .await
        .expect("the concurrent transaction opens");
    PgStore::lock_runner_type(&mut concurrent, fleet.id(), &key)
        .await
        .expect("the concurrent pod holds the fleet");

    // When: this pod reacts to instance-b's expiry and reaches the write behind that holder
    let store = fixture.store.clone();
    let contended_key = key.clone();
    let contended_instance = instance_b.clone();
    let losing = tokio::spawn(async move {
        presence::record_loss(
            &store,
            &UuidV7Factory,
            &SystemClock,
            &contended_key,
            &contended_instance,
            &ReasonCode::new(bc_jobs::commands::fleet::PRESENCE_EXPIRED)
                .expect("a valid reason code"),
        )
        .await
    });
    fixture.await_a_pod_blocked_on_the_fleet_lock().await;

    // When: the other pod's unrelated status change lands first, moving the whole fleet
    let reported = observe_presence(
        Some(&fleet),
        ObservePresence {
            runner_type_id: fleet.id(),
            runner_type: key.clone(),
            instance_key: InstanceKey::new("instance-a").expect("a valid instance key"),
            session_id: PresenceSessionId::new(ids().next()).expect("a v7 session id"),
            version: RunnerVersion::new("1.4.2").expect("a valid version"),
            reported_status: ReportedStatus::Draining,
            capacity: one_run(),
        },
    )
    .expect("the other pod decides the status change");
    let written: Vec<(EventId, FleetEvent)> = reported
        .events
        .iter()
        .map(|event| {
            (
                EventId::new(ids().next()).expect("a v7 event id"),
                event.clone(),
            )
        })
        .collect();
    apply::apply_fleet_events(
        &mut concurrent,
        fleet.id(),
        &written,
        &metadata(),
        Utc::now(),
    )
    .await
    .expect("the concurrent status change is written");
    concurrent
        .commit()
        .await
        .expect("the concurrent pod commits");

    // Then: the loss is not dropped — a moved fleet makes it re-decide, never abandon
    let outcome = losing.await.expect("the loss task finishes");
    assert!(
        matches!(outcome, Ok(true)),
        "a disconnection is delivered once and never replayed, so a fleet that moved under the \
         decision must be re-read, not treated as somebody else's write: {outcome:?}",
    );
    assert_eq!(
        fixture
            .count(
                "SELECT count(*) AS count FROM domain_events WHERE event_type = $1",
                "InstanceDisconnected",
            )
            .await,
        1,
        "the re-decided loss is recorded exactly once",
    );
    let settled = FleetReader::load(&fixture.store, &key)
        .await
        .expect("the fleet loads")
        .expect("the runner type still has live instances");
    assert!(
        settled.instance(&instance_b).is_none(),
        "the lost instance leaves the live fleet, so it stops electing work it can never run",
    );
    assert!(
        settled
            .instance(&InstanceKey::new("instance-a").expect("a valid instance key"))
            .is_some(),
        "the surviving instance keeps its own status change",
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_job_event_is_refused_when_the_run_lifecycle_moved_under_the_decision() {
    // Given: a job with one dispatched run, hydrated before any start
    let fixture = Fixture::start().await;
    let (decided_on, run_id) = a_job_with_one_dispatched_run(&fixture, "auditor").await;
    let instance = RunnerInstanceReference::new(
        RunnerTypeKey::new("auditor").expect("a valid runner type"),
        InstanceKey::new("instance-b").expect("a valid instance key"),
    );

    // When: the run starts, and only then does a decision taken on the older state try to commit
    let winner = decided_on
        .record_run_started(RunStartedFact {
            run_id,
            instance: instance.clone(),
        })
        .expect("the run start is decided");
    commit(
        &fixture,
        vec![JobChange::new(
            decided_on.id(),
            Some(decided_on.clone()),
            winner.events,
        )],
    )
    .await
    .expect("the first writer records the start");
    let latecomer = decided_on
        .record_run_started(RunStartedFact { run_id, instance })
        .expect("the stale decision still produces its fact");

    // Then: the write is refused because the run lifecycle no longer matches what was read
    let outcome = commit(
        &fixture,
        vec![JobChange::new(
            decided_on.id(),
            Some(decided_on),
            latecomer.events,
        )],
    )
    .await;
    assert!(
        matches!(outcome, Err(ServiceError::Contended)),
        "a decision read on a run without a start may not land after that run started: {outcome:?}",
    );
    assert_eq!(
        fixture
            .count(
                "SELECT count(*) AS count FROM domain_events WHERE event_type = $1",
                "RunStarted",
            )
            .await,
        1,
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_matching_lifecycle_still_admits_the_next_fact() {
    // Given: a job whose run has started, re-read after the write
    let fixture = Fixture::start().await;
    let (job, run_id) = a_job_with_one_dispatched_run(&fixture, "planner").await;
    let started = job
        .record_run_started(RunStartedFact {
            run_id,
            instance: RunnerInstanceReference::new(
                RunnerTypeKey::new("planner").expect("a valid runner type"),
                InstanceKey::new("instance-c").expect("a valid instance key"),
            ),
        })
        .expect("the run start is decided");
    commit(
        &fixture,
        vec![JobChange::new(job.id(), Some(job.clone()), started.events)],
    )
    .await
    .expect("the start is recorded");

    // When: the next fact is decided on the state as it now stands
    let current = JobReader::load(&fixture.store, job.id())
        .await
        .expect("the started job loads")
        .expect("the started job exists");
    let completed = current
        .record_run_completed(RunCompletedFact { run_id })
        .expect("the run completes");

    // Then: the guard admits it — it refuses stale decisions, never fresh ones
    commit(
        &fixture,
        vec![JobChange::new(job.id(), Some(current), completed.events)],
    )
    .await
    .expect("a decision taken on the current state is admitted");
    let settled = JobReader::load(&fixture.store, job.id())
        .await
        .expect("the settled job loads")
        .expect("the settled job exists");
    assert!(
        settled
            .find_run(run_id)
            .expect("the run is still there")
            .is_terminal(),
    );
    fixture.shutdown().await;
}
