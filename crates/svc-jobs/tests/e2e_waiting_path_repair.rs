mod support;

use br_test_harness::wait_until;
use support::db::{self, Durable};
use support::events::EventLog;
use support::fixture::{JobsFixture, Knobs};
use support::producer::{JobDeclaration, Producer};
use support::runner::{self, FakeRunner};
use support::{LONG, QUIET, wire};
use uuid::Uuid;

const NO_SWEEP_BEFORE_THE_RESTART_SECONDS: u64 = 3_600;

// A 0.3 pod still serving during a rolling update writes no waiting row for the jobs it admits,
// and removes none for the jobs it dispatches. The harness cannot run a 0.3 binary beside this
// one, so these test-only statements stand for exactly those two writes.
const FORGET_THE_WAITING_ROW: &str = "DELETE FROM waiting_jobs WHERE job_id = $1";
const LEAVE_A_STALE_WAITING_ROW: &str = "INSERT INTO waiting_jobs \
     (job_id, runner_type_id, root_job_id, effective_priority, created_at, due_at) \
     SELECT j.id, j.runner_type_id, coalesce(j.root_job_id, j.parent_job_id, j.id), \
            j.effective_priority, j.created_at, NULL \
     FROM jobs j WHERE j.id = $1";
const WAITING_ROWS_OF_JOB: &str = "SELECT count(*) FROM waiting_jobs WHERE job_id = $1";

#[tokio::test]
async fn the_backstop_sweep_repairs_the_waiting_path_left_by_an_older_pod() {
    // Given: a waiting job missing from the waiting path, and an executing job still on it
    let fixture = JobsFixture::start_with(Knobs {
        backstop_interval_seconds: 1,
        ..Knobs::default()
    })
    .await;
    let events = EventLog::open(fixture.fabric()).await;
    let durable = Durable::open(&fixture.app_url()).await;
    let producer = Producer::new(fixture.fabric(), "projects");
    let lost_type = wire::unique_runner_type("lost");
    let busy_type = wire::unique_runner_type("busy");

    let lost = JobDeclaration::new(&lost_type);
    producer.declare_v2(&lost).await;
    events
        .expect_one(wire::FACT_QUEUED, lost.job_id, LONG)
        .await;
    as_an_older_pod(&durable, FORGET_THE_WAITING_ROW, lost.job_id).await;

    let mut busy_instance = FakeRunner::new(fixture.nats(), &busy_type, "instance-a");
    busy_instance.connect().await;
    let executing = JobDeclaration::new(&busy_type);
    producer.declare_v2(&executing).await;
    let trigger = busy_instance.next_trigger(LONG).await;
    assert_eq!(runner::job_id(&trigger), executing.job_id);
    busy_instance.start_run(&trigger).await;
    events
        .expect_one(wire::FACT_STARTED, executing.job_id, LONG)
        .await;
    as_an_older_pod(&durable, LEAVE_A_STALE_WAITING_ROW, executing.job_id).await;

    // When: an instance of the lost job's type becomes available, and the backstop sweeps
    let mut lost_instance = FakeRunner::new(fixture.nats(), &lost_type, "instance-a");
    lost_instance.connect().await;

    // Then: the lost job is dispatched
    let dispatched = lost_instance.next_trigger(LONG).await;
    assert_eq!(runner::job_id(&dispatched), lost.job_id);
    assert_eq!(runner::attempt_number(&dispatched), 1);

    // Then: the stale row of the job that no longer waits is removed, and it is never
    // dispatched a second time
    assert_waiting_rows(&durable, executing.job_id, 0, "a job that no longer waits").await;
    busy_instance.expect_no_trigger(QUIET).await;
    durable
        .assert_count(db::RUNS_OF_JOB, executing.job_id, 1, "one run, never two")
        .await;

    durable.close().await;
    events.stop().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_restart_repairs_the_waiting_path_left_by_an_older_pod() {
    // Given: no backstop sweep before the restart, a waiting job missing from the waiting path,
    // and an executing job still on it
    let knobs = || Knobs {
        backstop_interval_seconds: NO_SWEEP_BEFORE_THE_RESTART_SECONDS,
        ..Knobs::default()
    };
    let mut fixture = JobsFixture::start_with(knobs()).await;
    let events = EventLog::open(fixture.fabric()).await;
    let durable = Durable::open(&fixture.app_url()).await;
    let producer = Producer::new(fixture.fabric(), "projects");
    let lost_type = wire::unique_runner_type("lost");
    let busy_type = wire::unique_runner_type("busy");

    let lost = JobDeclaration::new(&lost_type);
    producer.declare_v2(&lost).await;
    events
        .expect_one(wire::FACT_QUEUED, lost.job_id, LONG)
        .await;
    as_an_older_pod(&durable, FORGET_THE_WAITING_ROW, lost.job_id).await;

    let mut busy_instance = FakeRunner::new(fixture.nats(), &busy_type, "instance-a");
    busy_instance.connect().await;
    let executing = JobDeclaration::new(&busy_type);
    producer.declare_v2(&executing).await;
    let trigger = busy_instance.next_trigger(LONG).await;
    busy_instance.start_run(&trigger).await;
    events
        .expect_one(wire::FACT_STARTED, executing.job_id, LONG)
        .await;
    as_an_older_pod(&durable, LEAVE_A_STALE_WAITING_ROW, executing.job_id).await;
    // the instance keeps announcing itself after this handle goes
    drop(busy_instance);

    // When: the service restarts, then an instance of the lost job's type becomes available
    fixture.restart_first(&knobs()).await;
    let mut lost_instance = FakeRunner::new(fixture.nats(), &lost_type, "instance-a");
    lost_instance.connect().await;

    // Then: the lost job is dispatched, the stale row is removed, and the executing job is never
    // dispatched twice
    let dispatched = lost_instance.next_trigger(LONG).await;
    assert_eq!(runner::job_id(&dispatched), lost.job_id);
    assert_waiting_rows(&durable, executing.job_id, 0, "a job that no longer waits").await;
    tokio::time::sleep(QUIET).await;
    assert_eq!(
        FakeRunner::new(fixture.nats(), &busy_type, "observer")
            .trigger_count()
            .await,
        1,
        "the executing job was triggered once, never again after the restart",
    );
    durable
        .assert_count(db::RUNS_OF_JOB, executing.job_id, 1, "one run, never two")
        .await;

    durable.close().await;
    events.stop().await;
    fixture.shutdown().await;
}

async fn as_an_older_pod(durable: &Durable, statement: &str, job_id: Uuid) {
    let written = sqlx::query(statement)
        .bind(job_id)
        .execute(durable.pool())
        .await
        .unwrap_or_else(|error| panic!("reproducing an older pod's write `{statement}`: {error}"));
    assert_eq!(
        written.rows_affected(),
        1,
        "the older pod's write applies to exactly job {job_id}"
    );
}

async fn assert_waiting_rows(durable: &Durable, job_id: Uuid, expected: i64, what: &str) {
    let settled = wait_until(LONG, || async {
        durable.count(WAITING_ROWS_OF_JOB, job_id).await == expected
    })
    .await;
    assert!(
        settled,
        "{what}: job {job_id} must have {expected} waiting rows within {LONG:?}, has {}",
        durable.count(WAITING_ROWS_OF_JOB, job_id).await
    );
}
