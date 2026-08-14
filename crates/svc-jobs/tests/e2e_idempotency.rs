mod support;

use br_test_harness::SseSubscription;
use serde_json::json;
use support::events::EventLog;
use support::fixture::JobsFixture;
use support::producer::{JobDeclaration, Producer};
use support::runner::FakeRunner;
use support::{LONG, QUIET, SHORT, docs, gql, stream, wire};
use uuid::Uuid;

const JOB_CHANGED: &str = "jobsJobChanged";

#[tokio::test]
async fn delivery_retries_and_administrator_reconnection_do_not_duplicate_a_jobs_history() {
    let fixture = JobsFixture::start().await;
    let events = EventLog::open(fixture.fabric()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let runner_type = wire::unique_runner_type("idempotent");
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut instance = FakeRunner::new(fixture.nats(), &runner_type, "instance-a");
    instance.connect().await;

    let declaration = JobDeclaration::new(&runner_type).with_config(json!({ "seed": 1 }));
    let job_id = declaration.job_id;
    let command = producer.declare(&declaration).await;
    events.expect_one(wire::FACT_QUEUED, job_id, LONG).await;

    producer.redeliver(&command).await;
    producer.redeliver(&command).await;
    events
        .expect_exactly(wire::FACT_QUEUED, job_id, 1, QUIET)
        .await;

    let job = gql::job(&client, admin, job_id).await;
    assert_eq!(job["attemptCount"], json!(1));
    assert_eq!(job["runs"].as_array().map(Vec::len), Some(1));
    assert_eq!(
        instance.trigger_count().await,
        1,
        "an absorbed redelivery dispatches no second attempt",
    );

    let trigger = instance.next_trigger(LONG).await;
    let run = support::runner::run_id(&trigger);

    let mut watch = SseSubscription::open(
        fixture.url(),
        admin,
        &docs::job_changed_subscription(job_id),
    )
    .await;
    stream::snapshot(&mut watch, JOB_CHANGED, SHORT).await;

    instance.start_run(&trigger).await;
    stream::await_delta(&mut watch, JOB_CHANGED, "JobsRunStartedEvent", LONG).await;
    instance.start_run(&trigger).await;
    instance.start_run(&trigger).await;
    stream::expect_no_delta(&mut watch, JOB_CHANGED, "JobsRunStartedEvent", QUIET).await;
    events
        .expect_exactly(wire::FACT_STARTED, job_id, 1, QUIET)
        .await;

    let started = gql::job(&client, admin, job_id).await;
    let attempt = gql::run_by_attempt(&started, 1);
    let first_start = attempt["startedAt"].clone();
    assert_eq!(attempt["status"], json!("STARTED"));
    assert_eq!(started["attemptCount"], json!(1));

    let log_id = instance
        .log_line(&trigger, Some(0), "INFO", "one line, delivered twice")
        .await;
    instance
        .log_line(&trigger, Some(0), "INFO", "one line, delivered twice")
        .await;
    let logs = gql::logs_of(&client, admin, job_id).await;
    let repeated = logs
        .iter()
        .filter(|line| line["id"] == json!(log_id.to_string()))
        .count();
    assert!(
        repeated <= 1,
        "a redelivered log line is absorbed by its message id: {logs:?}"
    );

    instance.complete_run(&trigger).await;
    stream::await_delta(&mut watch, JOB_CHANGED, "JobsRunCompletedEvent", LONG).await;
    instance.complete_run(&trigger).await;
    stream::expect_no_delta(&mut watch, JOB_CHANGED, "JobsRunCompletedEvent", QUIET).await;

    let after_redelivery = gql::job(&client, admin, job_id).await;
    assert_eq!(
        gql::run_by_id(&after_redelivery, run)["startedAt"],
        first_start,
        "a redelivered lifecycle fact never rewrites history",
    );
    assert_eq!(after_redelivery["attemptCount"], json!(1));

    drop(watch);
    let mut reconnected = SseSubscription::open(
        fixture.url(),
        admin,
        &docs::job_changed_subscription(job_id),
    )
    .await;
    let snapshot = stream::snapshot(&mut reconnected, JOB_CHANGED, SHORT).await;
    assert_eq!(
        snapshot["job"]["status"],
        json!(gql::status_of(&client, admin, job_id).await),
        "a reconnection opens on a fresh snapshot equivalent to the read, not a replay",
    );
    assert_eq!(snapshot["job"]["attemptCount"], json!(1));
    stream::expect_no_delta(&mut reconnected, JOB_CHANGED, "JobsRunStartedEvent", QUIET).await;

    let mut tail =
        SseSubscription::open(fixture.url(), admin, &docs::log_tail_subscription(job_id)).await;
    let tailed = stream::snapshot(&mut tail, "jobsJobLogTail", SHORT).await;
    let mut ids: Vec<String> = tailed["logs"]["edges"]
        .as_array()
        .expect("a log snapshot carries its edges")
        .iter()
        .map(|edge| edge["node"]["id"].to_string())
        .collect();
    let before_dedup = ids.len();
    ids.sort();
    ids.dedup();
    assert_eq!(
        ids.len(),
        before_dedup,
        "a reconnecting log tail never repeats a line",
    );

    let resolution_id = Uuid::now_v7();
    producer.finish(job_id, resolution_id).await;
    producer.finish(job_id, resolution_id).await;
    gql::wait_for_status(&client, admin, job_id, "COMPLETED", LONG).await;
    events
        .expect_exactly(wire::FACT_COMPLETED, job_id, 1, QUIET)
        .await;

    events.stop().await;
    fixture.shutdown().await;
}
