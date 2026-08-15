mod support;

use std::time::Duration;

use br_test_harness::SseSubscription;
use chrono::Utc;
use serde_json::json;
use support::db::{self, Durable};
use support::events::EventLog;
use support::fixture::{JobsFixture, Knobs};
use support::producer::{JobDeclaration, Producer};
use support::runner::{self, FakeRunner};
use support::{
    FLEET_CHANGED, JOB_CHANGED, JOBS_CHANGED, LONG, QUIET, SHORT, delta, gql, stream, subs, wire,
};

const BASE_DELAY_SECONDS: i64 = 2;
const HINT_SECONDS: i64 = 8;
const SHORT_HINT_SECONDS: i64 = 1;

#[tokio::test]
async fn automatic_retries_honor_their_timing_and_stop_at_the_budget() {
    // Given: a job with exactly two permitted attempts, its first run started
    let fixture = JobsFixture::start_with(Knobs {
        retry_base_delay_seconds: BASE_DELAY_SECONDS as u64,
        max_attempts_ceiling: 3,
        ..Knobs::default()
    })
    .await;
    let events = EventLog::open(fixture.fabric()).await;
    let durable = Durable::open(&fixture.app_url()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let runner_type = wire::unique_runner_type("flaky");
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut instance = FakeRunner::new(fixture.nats(), &runner_type, "instance-a");
    instance.connect().await;

    let declaration = JobDeclaration::new(&runner_type).with_max_attempts(2);
    let job_id = declaration.job_id;
    producer.declare(&declaration).await;
    events.expect_one(wire::FACT_QUEUED, job_id, LONG).await;

    let first = instance.next_trigger(LONG).await;
    let first_run = runner::run_id(&first);
    instance.start_run(&first).await;
    gql::wait_for_status(&client, admin, job_id, "IN_PROGRESS", LONG).await;

    let mut watch = SseSubscription::open(fixture.url(), admin, &subs::job_changed(job_id)).await;
    stream::snapshot(&mut watch, JOB_CHANGED, SHORT).await;
    let mut listing =
        SseSubscription::open(fixture.url(), admin, &subs::jobs_changed(&runner_type)).await;
    stream::snapshot(&mut listing, JOBS_CHANGED, SHORT).await;
    let mut fleet_watch =
        SseSubscription::open(fixture.url(), admin, &subs::fleet_changed(&runner_type)).await;
    stream::snapshot(&mut fleet_watch, FLEET_CHANGED, SHORT).await;

    // When: the runner fails transiently with a hint longer than the ordinary backoff
    let failed_at = Utc::now();
    instance
        .fail_run(&first, "TRANSIENT", "provider_timeout", Some(HINT_SECONDS))
        .await;

    let run_failed = stream::await_delta(&mut watch, JOB_CHANGED, wire::EVT_RUN_FAILED, LONG).await;
    let failure = delta::event_of(&run_failed, wire::EVT_RUN_FAILED, job_id);
    assert_eq!(failure["runId"], json!(first_run.to_string()));
    assert_eq!(failure["failureKind"], json!("TRANSIENT"));
    assert_eq!(failure["reasonCode"], json!("provider_timeout"));

    let retry = stream::await_delta(&mut watch, JOB_CHANGED, wire::EVT_RETRY_SCHEDULED, LONG).await;
    let scheduling = delta::event_of(&retry, wire::EVT_RETRY_SCHEDULED, job_id);
    assert_eq!(scheduling["failedRunId"], json!(first_run.to_string()));
    let scheduled = delta::instant(&scheduling["dueAt"]);
    assert!(
        scheduled - failed_at >= chrono::Duration::seconds(HINT_SECONDS),
        "a runner retry-after hint lengthens the delay, never shortens it: due {scheduled}, \
         failed {failed_at}",
    );
    let pushed = delta::projection(&retry, job_id, "IN_PROGRESS");
    assert_eq!(
        delta::instant(&pushed["nextAttemptAt"]),
        scheduled,
        "the pushed projection carries the same recorded time as its event: {retry}",
    );
    delta::assert_active_affordances(&retry);

    // Then: the recorded time survives every read and a reconnection
    assert_eq!(
        delta::instant(&gql::job(&client, admin, job_id).await["nextAttemptAt"]),
        scheduled,
        "the backoff is computed once and stored, never recomputed on read",
    );
    drop(watch);
    let mut reconnected =
        SseSubscription::open(fixture.url(), admin, &subs::job_changed(job_id)).await;
    let snapshot = stream::snapshot(&mut reconnected, JOB_CHANGED, SHORT).await;
    assert_eq!(
        delta::instant(&snapshot["job"]["nextAttemptAt"]),
        scheduled,
        "a reconnection reconstructs the same recorded due time, with no jitter drift",
    );
    delta::assert_active_affordances(&snapshot);

    // Then: nothing is dispatched strictly before that time
    let quiet_until = (scheduled - Utc::now() - chrono::Duration::milliseconds(500))
        .to_std()
        .expect("the recorded due time is still ahead of the assertion window");
    instance.expect_no_trigger(quiet_until).await;

    let second = instance.next_trigger(LONG).await;
    assert!(
        Utc::now() >= scheduled,
        "the retry was dispatched before its recorded due time",
    );
    assert_eq!(runner::attempt_number(&second), 2);
    let second_run = runner::run_id(&second);
    let dispatched = stream::await_delta(
        &mut reconnected,
        JOB_CHANGED,
        wire::EVT_RUN_DISPATCHED,
        LONG,
    )
    .await;
    let dispatch = delta::event_of(&dispatched, wire::EVT_RUN_DISPATCHED, job_id);
    assert_eq!(dispatch["runId"], json!(second_run.to_string()));
    assert_eq!(dispatch["attemptNumber"], json!(2));
    let retried = gql::run_by_id(&gql::job(&client, admin, job_id).await, second_run);
    assert_eq!(retried["origin"], json!("AUTOMATIC_RETRY"));
    assert_eq!(
        retried["automaticRetryOfRunId"],
        json!(first_run.to_string())
    );

    // When: the second attempt fails too, exhausting the budget
    instance.start_run(&second).await;
    instance
        .fail_run(&second, "TRANSIENT", "provider_timeout", None)
        .await;

    let last_failure =
        stream::await_delta(&mut reconnected, JOB_CHANGED, wire::EVT_RUN_FAILED, LONG).await;
    assert_eq!(
        delta::event_of(&last_failure, wire::EVT_RUN_FAILED, job_id)["runId"],
        json!(second_run.to_string())
    );
    let job_failed =
        stream::await_delta(&mut reconnected, JOB_CHANGED, wire::EVT_JOB_FAILED, LONG).await;
    assert_eq!(
        delta::event_of(&job_failed, wire::EVT_JOB_FAILED, job_id)["failureCause"],
        json!("TERMINAL_RUN_FAILURE")
    );
    let terminal_projection = delta::projection(&job_failed, job_id, "FAILED");
    assert_eq!(
        terminal_projection["runs"].as_array().map(Vec::len),
        Some(2)
    );
    assert!(terminal_projection["activeRunId"].is_null());
    assert!(
        terminal_projection["nextAttemptAt"].is_null(),
        "an exhausted budget schedules nothing: {job_failed}",
    );
    delta::assert_failed_affordances(&job_failed);

    let exhausted = events.expect_one(wire::FACT_FAILED, job_id, LONG).await;
    assert_eq!(
        exhausted.payload()["failure_cause"],
        json!("TERMINAL_RUN_FAILURE")
    );
    assert_eq!(
        exhausted.payload()["failure_report"]["reason_code"],
        json!("provider_timeout")
    );

    let listed = stream::await_delta(&mut listing, JOBS_CHANGED, wire::EVT_JOB_FAILED, LONG).await;
    delta::assert_failed_affordances(&delta::assert_upserted_summary(&listed, job_id, "FAILED"));
    let last_attempt_stopped = stream::await_message(
        &mut fleet_watch,
        FLEET_CHANGED,
        "the last attempt leaving execution",
        |message| {
            message["event"]["kind"] == json!(wire::KIND_JOB_STOPPED_EXECUTING)
                && message["event"]["runId"] == json!(second_run.to_string())
        },
        LONG,
    )
    .await;
    delta::assert_fleet(&last_attempt_stopped, &runner_type, 0, 0, 0, 1);

    // Then: no third attempt is ever scheduled or dispatched
    stream::expect_no_delta(
        &mut reconnected,
        JOB_CHANGED,
        wire::EVT_RETRY_SCHEDULED,
        beyond_the_longest_delay(),
    )
    .await;
    instance.expect_no_trigger(QUIET).await;
    assert_eq!(
        instance.trigger_count().await,
        2,
        "the retry budget is a ceiling on dispatches, not on failures",
    );

    support::views::assert_views_agree(
        &durable,
        &client,
        admin,
        job_id,
        "a job whose retry budget is exhausted",
    )
    .await;

    // When: another job's runner asks to come back sooner than the backoff it earned
    let impatient_type = wire::unique_runner_type("impatient");
    let mut impatient = FakeRunner::new(fixture.nats(), &impatient_type, "instance-b");
    impatient.connect().await;
    let stubborn = JobDeclaration::new(&impatient_type).with_max_attempts(3);
    let stubborn_id = stubborn.job_id;
    producer.declare(&stubborn).await;
    events
        .expect_one(wire::FACT_QUEUED, stubborn_id, LONG)
        .await;

    let first_attempt = impatient.next_trigger(LONG).await;
    impatient.start_run(&first_attempt).await;
    impatient
        .fail_run(&first_attempt, "TRANSIENT", "provider_timeout", None)
        .await;
    let second_attempt = impatient.next_trigger(LONG).await;
    assert_eq!(runner::attempt_number(&second_attempt), 2);
    let second_attempt_run = runner::run_id(&second_attempt);
    impatient.start_run(&second_attempt).await;

    let mut stubborn_watch =
        SseSubscription::open(fixture.url(), admin, &subs::job_changed(stubborn_id)).await;
    stream::snapshot(&mut stubborn_watch, JOB_CHANGED, SHORT).await;

    let second_failed_at = Utc::now();
    impatient
        .fail_run(
            &second_attempt,
            "TRANSIENT",
            "provider_timeout",
            Some(SHORT_HINT_SECONDS),
        )
        .await;

    // Then: the too-short hint is ignored — the stored due time is the backoff, never the hint
    let hinted = stream::await_delta(
        &mut stubborn_watch,
        JOB_CHANGED,
        wire::EVT_RETRY_SCHEDULED,
        LONG,
    )
    .await;
    let hinted_scheduling = delta::event_of(&hinted, wire::EVT_RETRY_SCHEDULED, stubborn_id);
    assert_eq!(
        hinted_scheduling["failedRunId"],
        json!(second_attempt_run.to_string())
    );
    let hinted_due = delta::instant(&hinted_scheduling["dueAt"]);
    assert!(
        hinted_due - second_failed_at >= chrono::Duration::seconds(BASE_DELAY_SECONDS),
        "a runner retry-after hint may lengthen the delay but never shorten it: this second \
         failure earned an exponential backoff of at least the {BASE_DELAY_SECONDS}s base, and a \
         hint of {SHORT_HINT_SECONDS}s must not pull the attempt forward — obeying it is exactly \
         the retry storm the rule exists to forbid. Due {hinted_due}, failed {second_failed_at}",
    );
    let hinted_projection = delta::projection(&hinted, stubborn_id, "IN_PROGRESS");
    assert_eq!(
        delta::instant(&hinted_projection["nextAttemptAt"]),
        hinted_due,
        "the pushed projection carries the recorded time, not the hint: {hinted}",
    );

    let quiet_until = (hinted_due - Utc::now() - chrono::Duration::milliseconds(500))
        .to_std()
        .expect("the recorded due time is still ahead of the assertion window");
    impatient.expect_no_trigger(quiet_until).await;
    assert_eq!(
        delta::instant(&gql::job(&client, admin, stubborn_id).await["nextAttemptAt"]),
        hinted_due,
        "the read answers the same recorded due time as the stream",
    );

    let third_attempt = impatient.next_trigger(LONG).await;
    assert_eq!(runner::attempt_number(&third_attempt), 3);
    assert!(
        Utc::now() >= hinted_due,
        "the third attempt was dispatched before its recorded due time",
    );
    durable
        .assert_count(
            db::RETRY_SCHEDULES_OF_JOB,
            stubborn_id,
            2,
            "one stored due time per transient failure, each computed once — a due time recomputed \
             from the hint on read would move under the administrator's eyes",
        )
        .await;

    durable
        .assert_all(
            job_id,
            &[
                (db::RUNS_OF_JOB, 2, "two runs"),
                (
                    db::RETRY_SCHEDULES_OF_JOB,
                    1,
                    "one once-computed retry due time",
                ),
                (db::RESOLUTIONS_OF_JOB, 1, "one terminal resolution"),
            ],
        )
        .await;

    durable.close().await;
    events.stop().await;
    fixture.shutdown().await;
}

fn beyond_the_longest_delay() -> Duration {
    Duration::from_secs((HINT_SECONDS + BASE_DELAY_SECONDS) as u64)
}
