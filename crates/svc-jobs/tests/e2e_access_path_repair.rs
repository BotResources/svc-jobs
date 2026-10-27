//! The access paths a rolling update leaves behind: a 0.3 pod still serving
//! after migration 0003 records facts without maintaining `waiting_jobs` or
//! `unstarted_pushed_runs`. The drift is seeded directly in the database —
//! exactly what such a pod leaves — and the backstop sweep must repair it
//! from the ledger, in both directions.

mod support;

use std::time::Duration;

use support::db::Durable;
use support::events::EventLog;
use support::fixture::{JobsFixture, Knobs};
use support::producer::{JobDeclaration, Producer};
use support::runner::{self, FakeRunner};
use support::{LONG, wire};
use uuid::Uuid;

const WAITING_ROWS: &str = "SELECT count(*) FROM waiting_jobs WHERE job_id = $1";
const OUTSTANDING_TRIGGERS: &str = "SELECT count(*) FROM unstarted_pushed_runs WHERE job_id = $1";

async fn eventually_counts(durable: &Durable, sql: &str, job_id: Uuid, expected: i64, what: &str) {
    let deadline = tokio::time::Instant::now() + LONG;
    loop {
        if durable.count(sql, job_id).await == expected {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{what}: {sql} never reached {expected} for {job_id}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test]
async fn a_job_a_pod_left_off_the_waiting_path_is_repaired_and_dispatched() {
    // Given: a job waiting on a type nobody serves yet, the sweep running every second
    let fixture = JobsFixture::start_with(Knobs {
        backstop_interval_seconds: 1,
        ..Knobs::default()
    })
    .await;
    let events = EventLog::open(fixture.fabric()).await;
    let durable = Durable::open(&fixture.app_url()).await;
    let runner_type = wire::unique_runner_type("drifted");
    let producer = Producer::new(fixture.fabric(), "projects");
    let declaration = JobDeclaration::new(&runner_type);
    let job_id = declaration.job_id;
    producer.declare_v2(&declaration).await;
    events.expect_one(wire::FACT_QUEUED, job_id, LONG).await;
    durable
        .assert_count(
            WAITING_ROWS,
            job_id,
            1,
            "the admission writes the waiting row",
        )
        .await;

    // When: the waiting row disappears, as when a 0.3 pod admitted the job
    sqlx::query("DELETE FROM waiting_jobs WHERE job_id = $1")
        .bind(job_id)
        .execute(durable.pool())
        .await
        .expect("the drift is seeded");

    // Then: the sweep restores it from the ledger
    eventually_counts(&durable, WAITING_ROWS, job_id, 1, "the missing waiting row").await;

    // And: an instance of the type receives the job's trigger
    let mut instance = FakeRunner::new(fixture.nats(), &runner_type, "instance-a");
    instance.connect().await;
    let trigger = instance.next_trigger(LONG).await;
    assert_eq!(runner::job_id(&trigger), job_id);
    eventually_counts(
        &durable,
        WAITING_ROWS,
        job_id,
        0,
        "a dispatched job stops waiting",
    )
    .await;
    eventually_counts(
        &durable,
        OUTSTANDING_TRIGGERS,
        job_id,
        1,
        "the published trigger is outstanding",
    )
    .await;

    // When: stale rows appear for a job that no longer waits and a trigger already taken —
    // the job is put back on the waiting path, and its trigger is forgotten
    instance.start_run(&trigger).await;
    events.expect_one(wire::FACT_STARTED, job_id, LONG).await;
    eventually_counts(
        &durable,
        OUTSTANDING_TRIGGERS,
        job_id,
        0,
        "a started run is taken",
    )
    .await;
    sqlx::query(
        "INSERT INTO waiting_jobs (job_id, runner_type_id, root_job_id, effective_priority, \
            created_at) \
         SELECT id, runner_type_id, id, effective_priority, created_at FROM jobs WHERE id = $1",
    )
    .bind(job_id)
    .execute(durable.pool())
    .await
    .expect("the stale waiting row is seeded");

    // Then: the sweep removes it — the job is executing, it does not wait
    eventually_counts(&durable, WAITING_ROWS, job_id, 0, "the stale waiting row").await;
    instance.expect_no_trigger(Duration::from_secs(2)).await;

    events.stop().await;
    durable.close().await;
    fixture.shutdown().await;
}
