mod support;

use br_test_harness::{SseSubscription, verdict};
use serde_json::json;
use support::events::EventLog;
use support::fixture::{JobsFixture, Knobs};
use support::producer::{JobDeclaration, Producer};
use support::runner::{self, FakeRunner};
use support::{LONG, QUIET, SHORT, docs, gql, stream, wire};
use uuid::Uuid;

const RUN_MAX_DURATION_SECONDS: u64 = 3;
const INACTIVITY_TIMEOUT_SECONDS: u64 = 4;

#[tokio::test]
async fn safety_backstops_reclaim_abandoned_work() {
    let fixture = JobsFixture::start_with(Knobs {
        run_max_duration_seconds: RUN_MAX_DURATION_SECONDS,
        inactivity_timeout_seconds: INACTIVITY_TIMEOUT_SECONDS,
        backstop_interval_seconds: 1,
        ..Knobs::default()
    })
    .await;
    let events = EventLog::open(fixture.fabric()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let runner_type = wire::unique_runner_type("abandoning");
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut instance = FakeRunner::new(fixture.nats(), &runner_type, "instance-a");
    instance.connect().await;

    let abandoned_by_runner = JobDeclaration::new(&runner_type).with_max_attempts(1);
    let runner_job_id = abandoned_by_runner.job_id;
    producer.declare(&abandoned_by_runner).await;
    events
        .expect_one(wire::FACT_QUEUED, runner_job_id, LONG)
        .await;

    let mut watch = SseSubscription::open(
        fixture.url(),
        admin,
        &docs::job_changed_subscription(runner_job_id),
    )
    .await;
    stream::snapshot(&mut watch, "jobsJobChanged", SHORT).await;

    let trigger = instance.next_trigger(LONG).await;
    let stuck_run = runner::run_id(&trigger);
    instance.start_run(&trigger).await;
    gql::wait_for_status(&client, admin, runner_job_id, "IN_PROGRESS", LONG).await;

    let entry = instance.await_cancel_entry(stuck_run, LONG).await;
    assert_eq!(
        entry["run_id"],
        json!(stuck_run.to_string()),
        "a run past its maximum duration has its cancellation requested",
    );

    stream::await_delta(&mut watch, "jobsJobChanged", "JobsRunFailedEvent", LONG).await;
    stream::await_delta(&mut watch, "jobsJobChanged", "JobsJobFailedEvent", LONG).await;
    gql::wait_for_status(&client, admin, runner_job_id, "FAILED", LONG).await;
    let overrun = gql::job(&client, admin, runner_job_id).await;
    let reclaimed = gql::run_by_id(&overrun, stuck_run);
    assert_eq!(reclaimed["status"], json!("FAILED"));
    assert_eq!(
        reclaimed["failureReport"]["kind"],
        json!("TRANSIENT"),
        "an overrun run fails as transient, the retry budget decides the job's fate",
    );
    let reason = reclaimed["failureReport"]["reasonCode"]
        .as_str()
        .expect("a failure report carries a stable reason code");
    assert!(
        verdict::is_code_shaped(reason),
        "the backstop reason must be a code, not prose: {reason}"
    );
    assert!(!reclaimed["cancellationRequestedAt"].is_null());
    assert_eq!(
        overrun["resolution"]["failureCause"],
        json!("TERMINAL_RUN_FAILURE"),
        "with no budget left the transient failure ends the job",
    );

    instance.complete_run(&trigger).await;
    events
        .expect_none(wire::FACT_COMPLETED, runner_job_id, QUIET)
        .await;
    stream::expect_no_delta(&mut watch, "jobsJobChanged", "JobsRunCompletedEvent", QUIET).await;
    let after_late_fact = gql::job(&client, admin, runner_job_id).await;
    assert_eq!(after_late_fact["status"], json!("FAILED"));
    assert_ne!(
        gql::run_by_id(&after_late_fact, stuck_run)["status"],
        json!("COMPLETED"),
        "later status facts are ignored once the run was reclaimed",
    );
    assert_eq!(after_late_fact["attemptCount"], json!(1));

    let abandoned_by_owner = JobDeclaration::new(&runner_type).with_max_attempts(1);
    let owner_job_id = abandoned_by_owner.job_id;
    producer.declare(&abandoned_by_owner).await;
    events
        .expect_one(wire::FACT_QUEUED, owner_job_id, LONG)
        .await;
    let owner_trigger = instance.next_trigger(LONG).await;
    instance.start_run(&owner_trigger).await;
    instance.complete_run(&owner_trigger).await;
    gql::wait_for_status(&client, admin, owner_job_id, "IN_PROGRESS", LONG).await;

    let waiting = gql::job(&client, admin, owner_job_id).await;
    assert!(
        waiting["activeRunId"].is_null() && waiting["nextAttemptAt"].is_null(),
        "the job now waits on an owner who never answers: {waiting}",
    );

    gql::wait_for_status(&client, admin, owner_job_id, "FAILED", LONG).await;
    let timed_out = events
        .expect_one(wire::FACT_FAILED, owner_job_id, LONG)
        .await;
    assert_eq!(
        timed_out.payload()["failure_cause"],
        json!("INACTIVITY_TIMEOUT")
    );
    let inactive = gql::job(&client, admin, owner_job_id).await;
    assert_eq!(
        inactive["resolution"]["failureCause"],
        json!("INACTIVITY_TIMEOUT")
    );
    assert!(
        inactive["resolution"]["causedByRunId"].is_null(),
        "an inactivity timeout is not caused by a run: {inactive}",
    );

    producer.finish(owner_job_id, Uuid::now_v7()).await;
    events
        .expect_none(wire::FACT_COMPLETED, owner_job_id, QUIET)
        .await;
    assert_eq!(
        gql::job(&client, admin, owner_job_id).await["resolution"]["failureCause"],
        json!("INACTIVITY_TIMEOUT"),
        "a late owner decision never revives a terminal job",
    );
    instance.expect_no_trigger(QUIET).await;

    events.stop().await;
    fixture.shutdown().await;
}
