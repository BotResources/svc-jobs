mod support;

use std::time::Duration;

use br_test_harness::SseSubscription;
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use support::events::EventLog;
use support::fixture::{JobsFixture, Knobs};
use support::producer::{JobDeclaration, Producer};
use support::runner::{self, FakeRunner};
use support::{LONG, QUIET, SHORT, docs, gql, stream, wire};
use uuid::Uuid;

const JOB_CHANGED: &str = "jobsJobChanged";
const BASE_DELAY_SECONDS: i64 = 2;
const HINT_SECONDS: i64 = 6;

#[tokio::test]
async fn automatic_retries_honor_their_timing_and_stop_at_the_budget() {
    let fixture = JobsFixture::start_with(Knobs {
        retry_base_delay_seconds: BASE_DELAY_SECONDS as u64,
        max_attempts_ceiling: 3,
        ..Knobs::default()
    })
    .await;
    let events = EventLog::open(fixture.fabric()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let runner_type = wire::unique_runner_type("flaky");
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut instance = FakeRunner::new(fixture.nats(), &runner_type, "instance-a");
    instance.connect().await;

    let declaration = JobDeclaration::new(&runner_type).with_max_attempts(3);
    let job_id = declaration.job_id;
    producer.declare(&declaration).await;
    events.expect_one(wire::FACT_QUEUED, job_id, LONG).await;

    let mut watch = SseSubscription::open(
        fixture.url(),
        admin,
        &docs::job_changed_subscription(job_id),
    )
    .await;
    stream::snapshot(&mut watch, JOB_CHANGED, SHORT).await;

    let first = instance.next_trigger(LONG).await;
    let first_run = runner::run_id(&first);
    instance.start_run(&first).await;
    let failed_at = Utc::now();
    instance
        .fail_run(&first, "TRANSIENT", "provider_timeout", Some(0))
        .await;

    stream::await_delta(&mut watch, JOB_CHANGED, "JobsRunFailedEvent", LONG).await;
    let scheduled = wait_for_next_attempt(&client, admin, job_id).await;
    assert!(
        scheduled - failed_at >= chrono::Duration::seconds(BASE_DELAY_SECONDS),
        "a retry-after hint may lengthen the delay, never shorten it: due {scheduled}, failed {failed_at}",
    );
    let read_again = wait_for_next_attempt(&client, admin, job_id).await;
    assert_eq!(
        scheduled, read_again,
        "the backoff is computed once and stored, never recomputed on read",
    );
    let failed_run = gql::run_by_id(&gql::job(&client, admin, job_id).await, first_run);
    assert_eq!(failed_run["status"], json!("FAILED"));
    assert_eq!(failed_run["failureReport"]["kind"], json!("TRANSIENT"));
    assert_eq!(instant(&failed_run["retryDueAt"]), scheduled);

    instance.expect_no_trigger(Duration::from_millis(800)).await;
    assert_eq!(
        gql::status_of(&client, admin, job_id).await,
        "IN_PROGRESS",
        "a job awaiting its retry is still in progress, never terminal",
    );

    let second = instance.next_trigger(LONG).await;
    assert_eq!(runner::attempt_number(&second), 2);
    assert!(
        Utc::now() >= scheduled,
        "the retry was dispatched before its recorded due time",
    );
    let second_run = runner::run_id(&second);
    let retried = gql::run_by_id(&gql::job(&client, admin, job_id).await, second_run);
    assert_eq!(retried["origin"], json!("AUTOMATIC_RETRY"));
    assert_eq!(
        retried["automaticRetryOfRunId"],
        json!(first_run.to_string())
    );

    instance.start_run(&second).await;
    let hinted_at = Utc::now();
    instance
        .fail_run(&second, "TRANSIENT", "provider_timeout", Some(HINT_SECONDS))
        .await;
    let hinted = wait_for_next_attempt(&client, admin, job_id).await;
    assert!(
        hinted - hinted_at >= chrono::Duration::seconds(HINT_SECONDS),
        "the runner's retry-after hint lengthens the delay: due {hinted}, failed {hinted_at}",
    );

    let third = instance.next_trigger(LONG).await;
    assert_eq!(runner::attempt_number(&third), 3);
    instance.start_run(&third).await;
    instance
        .fail_run(&third, "TRANSIENT", "provider_timeout", None)
        .await;

    gql::wait_for_status(&client, admin, job_id, "FAILED", LONG).await;
    let exhausted = events.expect_one(wire::FACT_FAILED, job_id, LONG).await;
    assert_eq!(
        exhausted.payload()["failure_cause"],
        json!("TERMINAL_RUN_FAILURE")
    );
    assert_eq!(
        exhausted.payload()["failure_report"]["reason_code"],
        json!("provider_timeout")
    );
    stream::await_delta(&mut watch, JOB_CHANGED, "JobsJobFailedEvent", LONG).await;

    let terminal = gql::job_view(&client, admin, job_id).await;
    assert_eq!(terminal["job"]["attemptCount"], json!(3));
    assert!(
        terminal["job"]["nextAttemptAt"].is_null(),
        "an exhausted budget schedules nothing: {terminal}",
    );
    gql::assert_blocked(&terminal, wire::ACTION_CANCEL);
    gql::assert_allowed(&terminal, wire::ACTION_MANUAL_RETRY);

    instance.expect_no_trigger(QUIET).await;
    assert_eq!(
        instance.trigger_count().await,
        3,
        "the retry budget is a ceiling on dispatches, not on failures",
    );

    events.stop().await;
    fixture.shutdown().await;
}

async fn wait_for_next_attempt(
    client: &br_test_harness::GraphqlClient,
    admin: &br_core_auth::Passport,
    job_id: Uuid,
) -> DateTime<Utc> {
    let deadline = tokio::time::Instant::now() + LONG;
    loop {
        let job = gql::job(client, admin, job_id).await;
        if !job["nextAttemptAt"].is_null() {
            return instant(&job["nextAttemptAt"]);
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "job {job_id} never recorded a next attempt time: {job}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn instant(value: &Value) -> DateTime<Utc> {
    let raw = value
        .as_str()
        .unwrap_or_else(|| panic!("expected an RFC3339 timestamp, got: {value}"));
    DateTime::parse_from_rfc3339(raw)
        .unwrap_or_else(|e| panic!("a DateTime must be RFC3339: {raw} ({e})"))
        .with_timezone(&Utc)
}
