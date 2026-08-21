mod support;

use br_test_harness::SseSubscription;
use serde_json::json;
use support::db::{self, Durable};
use support::events::EventLog;
use support::fixture::{JobsFixture, Knobs};
use support::producer::{JobDeclaration, Producer};
use support::runner::{self, FakeRunner};
use support::{
    FLEET_CHANGED, JOB_CHANGED, JOBS_CHANGED, LOG_TAIL, LONG, QUIET, SHORT, codes, delta, gql,
    stream, subs, wire,
};

const RUN_MAX_DURATION_SECONDS: u64 = 3;
const INACTIVITY_TIMEOUT_SECONDS: u64 = 4;

#[tokio::test]
async fn safety_backstops_reclaim_abandoned_work() {
    // Given: two one-attempt jobs on an available runner type, both watched
    let fixture = JobsFixture::start_with(Knobs {
        run_max_duration_seconds: RUN_MAX_DURATION_SECONDS,
        inactivity_timeout_seconds: INACTIVITY_TIMEOUT_SECONDS,
        backstop_interval_seconds: 1,
        ..Knobs::default()
    })
    .await;
    let events = EventLog::open(fixture.fabric()).await;
    let durable = Durable::open(&fixture.app_url()).await;
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

    let mut watch =
        SseSubscription::open(fixture.url(), admin, &subs::job_changed(runner_job_id)).await;
    stream::snapshot(&mut watch, JOB_CHANGED, SHORT).await;
    let mut listing =
        SseSubscription::open(fixture.url(), admin, &subs::jobs_changed(&runner_type)).await;
    stream::snapshot(&mut listing, JOBS_CHANGED, SHORT).await;
    let mut tail =
        SseSubscription::open(fixture.url(), admin, &subs::log_tail(runner_job_id)).await;
    stream::snapshot(&mut tail, LOG_TAIL, SHORT).await;
    let mut fleet_watch =
        SseSubscription::open(fixture.url(), admin, &subs::fleet_changed(&runner_type)).await;
    stream::snapshot(&mut fleet_watch, FLEET_CHANGED, SHORT).await;

    // When: job A's run overruns the configured maximum duration
    let trigger = instance.next_trigger(LONG).await;
    let stuck_run = runner::run_id(&trigger);
    instance.start_run(&trigger).await;
    gql::wait_for_status(&client, admin, runner_job_id, "IN_PROGRESS", LONG).await;

    let entry = instance.await_cancel_entry(stuck_run, LONG).await;
    assert_eq!(
        entry["run_id"],
        json!(stuck_run.to_string()),
        "a run past its maximum duration has its cancellation requested at its own key",
    );

    let run_failed = stream::await_delta(&mut watch, JOB_CHANGED, wire::EVT_RUN_FAILED, LONG).await;
    let overrun = delta::event_of(&run_failed, wire::EVT_RUN_FAILED, runner_job_id);
    assert_eq!(overrun["runId"], json!(stuck_run.to_string()));
    assert_eq!(
        overrun["failureKind"],
        json!("TRANSIENT"),
        "an overrun run fails as transient — the retry budget decides the job's fate",
    );
    codes::assert_stable_reason(
        overrun["reasonCode"]
            .as_str()
            .expect("a failed-run event names its reason"),
        "the maximum-run-duration backstop",
    );

    let job_failed = stream::await_delta(&mut watch, JOB_CHANGED, wire::EVT_JOB_FAILED, LONG).await;
    assert_eq!(
        delta::event_of(&job_failed, wire::EVT_JOB_FAILED, runner_job_id)["failureCause"],
        json!("TERMINAL_RUN_FAILURE"),
        "with no budget left the transient failure ends the job",
    );
    delta::projection(&job_failed, runner_job_id, "FAILED");
    delta::assert_failed_affordances(&job_failed);

    let escalated = events
        .expect_one(wire::FACT_FAILED, runner_job_id, LONG)
        .await;
    assert_eq!(
        escalated.payload()["failure_cause"],
        json!("TERMINAL_RUN_FAILURE"),
        "the producing service learns from the backstop that its work is dead — an owner never \
         told would wait forever on a job reclaimed long ago",
    );
    assert_eq!(
        escalated.payload()["failure_report"]["kind"],
        json!("TRANSIENT"),
        "the reclaimed run's report is escalated to the owner as-is",
    );
    assert_eq!(
        escalated.payload()["failure_report"]["reason_code"],
        overrun["reasonCode"],
        "the backstop's own reason reaches the owner unchanged",
    );
    delta::assert_failed_affordances(&delta::assert_upserted_summary(
        &stream::await_delta(&mut listing, JOBS_CHANGED, wire::EVT_JOB_FAILED, LONG).await,
        runner_job_id,
        "FAILED",
    ));
    delta::assert_fleet(
        &stream::await_fleet_event(&mut fleet_watch, wire::KIND_JOB_STOPPED_EXECUTING, LONG).await,
        &runner_type,
        0,
        0,
        0,
        1,
    );

    let reclaimed = gql::run_by_id(&gql::job(&client, admin, runner_job_id).await, stuck_run);
    assert_eq!(reclaimed["status"], json!("FAILED"));
    assert!(!reclaimed["cancellationRequestedAt"].is_null());
    instance.await_no_cancel_entry(stuck_run, LONG).await;

    // When: the abandoning runner reports every late fact it owes
    instance.start_run(&trigger).await;
    instance.complete_run(&trigger).await;
    instance
        .fail_run(&trigger, "PERMANENT", "late_noise", None)
        .await;
    events
        .expect_none(wire::FACT_COMPLETED, runner_job_id, QUIET)
        .await;
    stream::expect_no_delta(&mut watch, JOB_CHANGED, wire::EVT_RUN_COMPLETED, QUIET).await;
    let after_late_facts = gql::job(&client, admin, runner_job_id).await;
    assert_eq!(after_late_facts["status"], json!("FAILED"));
    assert_ne!(
        gql::run_by_id(&after_late_facts, stuck_run)["status"],
        json!("COMPLETED"),
        "later status facts are ignored once the run was reclaimed",
    );
    assert_eq!(after_late_facts["attemptCount"], json!(1));

    // When: job B's run completes but its owner never decides
    let abandoned_by_owner = JobDeclaration::new(&runner_type).with_max_attempts(1);
    let owner_job_id = abandoned_by_owner.job_id;
    producer.declare(&abandoned_by_owner).await;
    events
        .expect_one(wire::FACT_QUEUED, owner_job_id, LONG)
        .await;
    let mut owner_watch =
        SseSubscription::open(fixture.url(), admin, &subs::job_changed(owner_job_id)).await;
    stream::snapshot(&mut owner_watch, JOB_CHANGED, SHORT).await;

    let owner_trigger = instance.next_trigger(LONG).await;
    instance.start_run(&owner_trigger).await;
    instance.complete_run(&owner_trigger).await;
    let run_completed =
        stream::await_delta(&mut owner_watch, JOB_CHANGED, wire::EVT_RUN_COMPLETED, LONG).await;
    delta::projection(&run_completed, owner_job_id, "IN_PROGRESS");
    events
        .expect_none(wire::FACT_COMPLETED, owner_job_id, QUIET)
        .await;

    let waiting = gql::job(&client, admin, owner_job_id).await;
    assert!(
        waiting["activeRunId"].is_null() && waiting["nextAttemptAt"].is_null(),
        "the job now waits on an owner who never answers: {waiting}",
    );

    let timed_out =
        stream::await_delta(&mut owner_watch, JOB_CHANGED, wire::EVT_JOB_FAILED, LONG).await;
    assert_eq!(
        delta::event_of(&timed_out, wire::EVT_JOB_FAILED, owner_job_id)["failureCause"],
        json!("INACTIVITY_TIMEOUT")
    );
    delta::projection(&timed_out, owner_job_id, "FAILED");
    delta::assert_failed_affordances(&timed_out);
    assert_eq!(
        events
            .expect_one(wire::FACT_FAILED, owner_job_id, LONG)
            .await
            .payload()["failure_cause"],
        json!("INACTIVITY_TIMEOUT")
    );
    let inactive = gql::job(&client, admin, owner_job_id).await;
    assert!(
        inactive["resolution"]["causedByRunId"].is_null(),
        "an inactivity timeout is not caused by a run: {inactive}",
    );

    // When: both owners answer far too late
    for job_id in [runner_job_id, owner_job_id] {
        producer.finish(job_id).await;
        producer.fail(job_id, None).await;
    }
    events
        .expect_none(wire::FACT_COMPLETED, runner_job_id, QUIET)
        .await;
    events
        .expect_none(wire::FACT_COMPLETED, owner_job_id, QUIET)
        .await;
    events
        .expect_exactly(wire::FACT_FAILED, owner_job_id, 1, QUIET)
        .await;
    events
        .expect_exactly(wire::FACT_FAILED, runner_job_id, 1, QUIET)
        .await;
    for (job_id, cause) in [
        (runner_job_id, "TERMINAL_RUN_FAILURE"),
        (owner_job_id, "INACTIVITY_TIMEOUT"),
    ] {
        assert_eq!(
            gql::job(&client, admin, job_id).await["resolution"]["failureCause"],
            json!(cause),
            "a late owner decision never replaces a terminal resolution",
        );
    }
    instance.expect_no_trigger(QUIET).await;
    stream::expect_total_silence(&mut tail, "a reclaimed run appends no log line", QUIET).await;

    // Then: the durable record holds exactly the timeout facts and one terminal resolution each
    for job_id in [runner_job_id, owner_job_id] {
        durable
            .assert_all(
                job_id,
                &[
                    (db::RUNS_OF_JOB, 1, "the single attempt its budget allowed"),
                    (
                        db::RESOLUTIONS_OF_JOB,
                        1,
                        "one terminal resolution — a second row would let a 'last one wins' read \
                         hide a late owner decision that overwrote the backstop's",
                    ),
                ],
            )
            .await;
    }

    durable.close().await;
    events.stop().await;
    fixture.shutdown().await;
}
