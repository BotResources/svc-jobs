mod support;

use bc_jobs::domain::attempts::MaxAttempts;
use bc_jobs::domain::auto_finish::{AnswerRecord, CallerRun, CallerRunEnd};
use bc_jobs::domain::policy::ServiceLimits;
use bc_jobs::domain::run::status::RunTerminalKind;
use bc_jobs::ports::auto_finish::AutoFinishReader;
use br_core_events::{Actor, EventMetadata, ServiceAccountId};
use chrono::TimeDelta;
use sqlx::PgPool;
use support::auto_finish::{
    Restartable, complete, mark_the_mail, reply_to, restartable, send_answer,
};
use support::call_notices::{Caller, caller};
use support::fixture::{JobsFixture, Knobs};
use support::restart::RESTARTS_OF_RUN;
use support::{LONG, clock, gql};
use svc_jobs::ServiceError;
use svc_jobs::app::environment::UuidV7Factory;
use svc_jobs::app::write::{self, JobChange};
use svc_jobs::db::PgStore;
use uuid::Uuid;

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

async fn quiet_fixture() -> JobsFixture {
    JobsFixture::start_with(Knobs {
        backstop_interval_seconds: 3600,
        ..Knobs::default()
    })
    .await
}

async fn store_of(fixture: &JobsFixture) -> PgStore {
    PgStore::new(
        PgPool::connect(&fixture.database().owner_migration_url())
            .await
            .expect("the owner connection opens"),
    )
}

async fn answered_and_read(fixture: &JobsFixture, caller: &mut Caller<'_>) -> Restartable {
    let m = restartable(fixture, caller, true).await;
    let answer = send_answer(fixture, caller, &m.called, m.agent).await;
    complete(caller, &m.called).await;
    mark_the_mail(fixture, caller, answer).await;
    m
}

async fn stale_finish(
    store: &PgStore,
    m: &Restartable,
    caller: &Caller<'_>,
) -> Result<(), ServiceError> {
    let ended = br_test_harness::wait_until(LONG, || async {
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM run_terminals WHERE run_id = $1")
            .bind(m.called.work.run_id)
            .fetch_one(store.pool())
            .await
            .is_ok_and(|count| count == 1)
    })
    .await;
    assert!(ended, "the Run of the called job has completed");
    let mut facts = store
        .facts_of(bc_jobs::domain::ids::JobId::new(m.called.job).unwrap())
        .await
        .expect("the facts load")
        .expect("the called job exists");
    let now = clock::now();
    facts.restart_jobs = Vec::new();
    facts.restarts_due = Vec::new();
    facts.answers = vec![AnswerRecord {
        run_id: bc_jobs::domain::ids::RunId::new(m.called.work.run_id).unwrap(),
        recorded_at: now,
        read_at: Some(now),
    }];
    facts.caller_runs = vec![CallerRun {
        run_id: bc_jobs::domain::ids::RunId::new(caller.work.run_id).unwrap(),
        ended: Some(CallerRunEnd {
            kind: RunTerminalKind::Completed,
            at: now + TimeDelta::seconds(1),
        }),
    }];
    let plan = facts.decide().expect("the stale facts allow the finish");
    let result = facts
        .called
        .finish(
            plan.finish_command(bc_jobs::domain::ids::ResolutionId::new(Uuid::now_v7()).unwrap()),
        )
        .expect("the job finishes");
    let metadata = EventMetadata::new(
        Actor::Service(ServiceAccountId::from(svc_jobs::app::SERVICE_ACTOR)),
        Uuid::now_v7(),
    );
    write::commit_job_changes(
        store,
        write::WriteContext {
            ids: &UuidV7Factory,
            metadata: &metadata,
            at: now,
            limits: &limits(),
        },
        vec![
            JobChange::new(facts.called.id(), Some(facts.called.clone()), result.events)
                .finishing_by_rule(plan.premise().clone()),
        ],
    )
    .await
}

async fn hold_a_due_wake(store: &PgStore, run: Uuid) -> sqlx::Transaction<'static, sqlx::Postgres> {
    loop {
        sqlx::query(
            "INSERT INTO restart_wakes (run_id, requested_at) VALUES ($1, now()) \
             ON CONFLICT DO NOTHING",
        )
        .bind(run)
        .execute(store.pool())
        .await
        .expect("a due restart is recorded");
        let mut held = store.pool().begin().await.expect("a transaction opens");
        let locked = sqlx::query("SELECT 1 FROM restart_wakes WHERE run_id = $1 FOR UPDATE")
            .bind(run)
            .fetch_optional(&mut *held)
            .await
            .expect("the wake row is read");
        if locked.is_some() {
            return held;
        }
    }
}

#[tokio::test]
async fn a_restart_job_created_between_the_scan_and_the_commit_blocks_the_finish() {
    let fixture = quiet_fixture().await;
    let store = store_of(&fixture).await;
    let durable = support::db::Durable::open(&fixture.app_url()).await;
    let mut caller = caller(&fixture, "auto-race-job").await;
    let m = answered_and_read(&fixture, &mut caller).await;

    reply_to(&fixture, &caller, m.agent).await;
    durable
        .await_count(
            RESTARTS_OF_RUN,
            m.called.work.run_id,
            1,
            "the reply restarts",
        )
        .await;

    let refused = stale_finish(&store, &m, &caller).await;

    assert!(
        matches!(&refused, Err(error) if error.is_write_race()),
        "the commit refuses a finish decided before the restart job existed: {refused:?}"
    );
    assert_eq!(
        gql::status_of(&fixture.gql(), fixture.admin(), m.called.job).await,
        "IN_PROGRESS"
    );
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_restart_that_became_due_between_the_scan_and_the_commit_blocks_the_finish() {
    let fixture = quiet_fixture().await;
    let store = store_of(&fixture).await;
    let mut caller = caller(&fixture, "auto-race-wake").await;
    let m = answered_and_read(&fixture, &mut caller).await;
    let mut held = hold_a_due_wake(&store, m.called.work.run_id).await;

    let refused = stale_finish(&store, &m, &caller).await;

    assert!(
        matches!(&refused, Err(error) if error.is_write_race()),
        "the commit refuses a finish decided before the restart was due: {refused:?}"
    );
    assert_eq!(
        gql::status_of(&fixture.gql(), fixture.admin(), m.called.job).await,
        "IN_PROGRESS"
    );

    sqlx::query("DELETE FROM restart_wakes WHERE run_id = $1")
        .bind(m.called.work.run_id)
        .execute(&mut *held)
        .await
        .expect("the due restart is withdrawn");
    held.commit().await.expect("the withdrawal is committed");
    stale_finish(&store, &m, &caller)
        .await
        .expect("the same finish lands once nothing changed since the decision");
    gql::wait_for_status(
        &fixture.gql(),
        fixture.admin(),
        m.called.job,
        "COMPLETED",
        LONG,
    )
    .await;
    fixture.shutdown().await;
}
