mod support;

use bc_jobs::commands::job::cancellation::CancelJob;
use bc_jobs::commands::job::create::{CreateJob, CreateOutcome, create_job};
use bc_jobs::domain::attempts::MaxAttempts;
use bc_jobs::domain::ids::{JobId, ResolutionId};
use bc_jobs::domain::job::Job;
use bc_jobs::domain::keys::{ProducerKey, RunnerTypeKey};
use bc_jobs::domain::ownership::CancelRequester;
use bc_jobs::domain::policy::ServiceLimits;
use bc_jobs::ports::job::JobReader;
use br_core_events::{Actor, EventMetadata, ServiceAccountId};
use br_test_harness::E2eDatabase;
use chrono::TimeDelta;
use sqlx::{PgPool, Row};
use svc_jobs::app::environment::UuidV7Factory;
use svc_jobs::app::write::{self, JobChange};
use svc_jobs::db::PgStore;
use uuid::Uuid;

use support::clock;
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

    async fn affordance_events_for(&self, job_id: JobId) -> i64 {
        sqlx::query(
            "SELECT count(*) AS count FROM domain_events \
             WHERE event_type = 'JobAffordancesChanged' AND aggregate_id = $1",
        )
        .bind(job_id.as_uuid())
        .fetch_one(&self.pool)
        .await
        .expect("the count query runs")
        .get("count")
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

async fn commit(fixture: &Fixture, changes: Vec<JobChange>) {
    write::commit_job_changes(
        &fixture.store,
        &UuidV7Factory,
        changes,
        &metadata(),
        clock::now(),
    )
    .await
    .expect("the write lands");
}

async fn a_queued_job(fixture: &Fixture, parent: Option<&Job>) -> Job {
    let job_id = JobId::new(Uuid::now_v7()).expect("a v7 job id");
    let declaration = CreateJob {
        id: job_id,
        runner_type: RunnerTypeKey::new("archivist").expect("a valid runner type"),
        producer: ProducerKey::new("projects").expect("a valid producer"),
        config: None,
        parent_job_id: parent.map(Job::id),
        triggered_by: None,
        source_entity_id: None,
        max_attempts: None,
    };
    let CreateOutcome::Queued(queued) = create_job(declaration, None, None, parent, &limits())
        .expect("the declaration is accepted")
    else {
        panic!("a fresh job id is queued, never already queued");
    };
    commit(fixture, vec![JobChange::new(job_id, None, queued.events)]).await;
    JobReader::load(&fixture.store, job_id)
        .await
        .expect("the queued job loads")
        .expect("the queued job exists")
}

async fn cancel(fixture: &Fixture, job: &Job, originating_job_id: JobId) {
    let result = job
        .cancel(CancelJob {
            resolution_id: ResolutionId::new(Uuid::now_v7()).expect("a v7 resolution id"),
            requester: CancelRequester::Cascade { originating_job_id },
        })
        .expect("a queued job cancels");
    commit(
        fixture,
        vec![JobChange::new(job.id(), Some(job.clone()), result.events)],
    )
    .await;
}

#[tokio::test]
async fn settling_a_parent_records_its_descendants_affordance_change_in_the_same_write() {
    // Given: a parent whose child is already terminal, so settling the parent changes what the
    // child affords
    let fixture = Fixture::start().await;
    let parent = a_queued_job(&fixture, None).await;
    let child = a_queued_job(&fixture, Some(&parent)).await;
    cancel(&fixture, &child, child.id()).await;
    assert_eq!(
        fixture.affordance_events_for(child.id()).await,
        0,
        "nothing has changed the child's affordances yet",
    );
    let parent = JobReader::load(&fixture.store, parent.id())
        .await
        .expect("the parent loads")
        .expect("the parent exists");

    // When: the parent settles through the write path alone, with no follow-up call behind it
    cancel(&fixture, &parent, parent.id()).await;

    // Then: the child's affordance change is already recorded — it rode the settling transaction
    // instead of a second one a crash could swallow
    assert_eq!(
        fixture.affordance_events_for(child.id()).await,
        1,
        "settling a parent must record the descendant affordance change it causes in the very \
         transaction that settles it",
    );
    fixture.shutdown().await;
}
