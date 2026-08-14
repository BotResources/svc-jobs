mod support;

use br_test_harness::{SseSubscription, verdict};
use serde_json::json;
use support::events::EventLog;
use support::fixture::JobsFixture;
use support::producer::{JobDeclaration, Producer};
use support::runner::{self, FakeRunner};
use support::{LONG, QUIET, SHORT, docs, gql, stream, wire};
use uuid::Uuid;

const JOB_CHANGED: &str = "jobsJobChanged";

#[tokio::test]
async fn an_administrator_manually_retries_a_job_after_its_owner_accepts_a_terminal_failure() {
    let fixture = JobsFixture::start().await;
    let events = EventLog::open(fixture.fabric()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let runner_type = wire::unique_runner_type("retryable");
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut instance = FakeRunner::new(fixture.nats(), &runner_type, "instance-a");
    instance.connect().await;

    let entity_id = Uuid::now_v7();
    let operator_id = Uuid::now_v7();
    let declaration = JobDeclaration::new(&runner_type)
        .with_config(json!({ "prompt": "convert" }))
        .triggered_by(operator_id, "Amelie")
        .with_source("projects", entity_id)
        .with_max_attempts(1);
    let predecessor_id = declaration.job_id;
    producer.declare(&declaration).await;
    events
        .expect_one(wire::FACT_QUEUED, predecessor_id, LONG)
        .await;

    let mut watch = SseSubscription::open(
        fixture.url(),
        admin,
        &docs::job_changed_subscription(predecessor_id),
    )
    .await;
    stream::snapshot(&mut watch, JOB_CHANGED, SHORT).await;

    let trigger = instance.next_trigger(LONG).await;
    let failed_run = runner::run_id(&trigger);
    instance.start_run(&trigger).await;
    instance.declare_plan(&trigger, &["convert"]).await;
    instance.start_step(&trigger, 0, "convert").await;
    instance
        .log_line(&trigger, Some(0), "ERROR", "unsupported format")
        .await;
    instance
        .fail_run(&trigger, "PERMANENT", "unsupported_format", None)
        .await;

    gql::wait_for_status(&client, admin, predecessor_id, "FAILED", LONG).await;
    let failure = events
        .expect_one(wire::FACT_FAILED, predecessor_id, LONG)
        .await;
    assert_eq!(
        failure.payload()["failure_report"]["reason_code"],
        json!("unsupported_format")
    );
    instance.expect_no_trigger(QUIET).await;

    let failed_view = gql::job_view(&client, admin, predecessor_id).await;
    let failed_resolution_id = Uuid::parse_str(
        failed_view["job"]["resolution"]["id"]
            .as_str()
            .expect("a failed job carries its resolution id"),
    )
    .expect("a resolution id is a UUID");
    assert_eq!(
        failed_view["job"]["resolution"]["causedByRunId"],
        json!(failed_run.to_string())
    );
    gql::assert_allowed(&failed_view, wire::ACTION_MANUAL_RETRY);
    gql::assert_blocked(&failed_view, wire::ACTION_CANCEL);
    gql::assert_allowed(&failed_view, wire::ACTION_DELETE);

    let intervention_id = Uuid::now_v7();
    let successor_id = Uuid::now_v7();
    let ack = gql::manual_retry_job(
        &client,
        admin,
        intervention_id,
        predecessor_id,
        successor_id,
        failed_resolution_id,
    )
    .await;
    verdict::expect_ack(&ack, "an administrator retries an eligible failed job");
    assert_eq!(
        ack["data"]["jobsManualRetryJob"],
        json!({ "success": true })
    );
    stream::await_delta(&mut watch, JOB_CHANGED, "JobsManualRetryStartedEvent", LONG).await;

    events
        .expect_one(wire::FACT_QUEUED, successor_id, LONG)
        .await;
    gql::wait_for_attempts(&client, admin, successor_id, 1, LONG).await;
    let successor = gql::job(&client, admin, successor_id).await;
    assert_eq!(successor["runnerType"], json!(runner_type));
    assert_eq!(successor["config"], json!({ "prompt": "convert" }));
    assert_eq!(successor["maxAttempts"], json!(1));
    assert_eq!(
        successor["triggeredBy"]["id"],
        json!(operator_id.to_string())
    );
    assert_eq!(
        successor["source"]["entityId"],
        json!(entity_id.to_string())
    );
    assert_eq!(
        successor["status"],
        json!("IN_PROGRESS"),
        "the successor's first run is dispatched even though the predecessor exhausted its budget",
    );
    let successor_trigger = instance.next_trigger(LONG).await;
    assert_eq!(runner::job_id(&successor_trigger), successor_id);
    assert_eq!(runner::attempt_number(&successor_trigger), 1);

    let predecessor = gql::job(&client, admin, predecessor_id).await;
    assert_eq!(predecessor["status"], json!("FAILED"));
    assert_eq!(predecessor["attemptCount"], json!(1));
    assert_eq!(
        predecessor["resolution"]["id"],
        json!(failed_resolution_id.to_string()),
        "a manual retry never reopens the predecessor",
    );
    assert_eq!(
        predecessor["manualRetry"],
        json!({
            "id": intervention_id.to_string(),
            "failedResolutionId": failed_resolution_id.to_string(),
            "predecessorJobId": predecessor_id.to_string(),
            "successorJobId": successor_id.to_string(),
            "requestedAt": predecessor["manualRetry"]["requestedAt"],
            "requestedBy": {
                "id": admin.actor_id().to_string(),
                "displayName": predecessor["manualRetry"]["requestedBy"]["displayName"],
            }
        }),
        "the human intervention is recorded on the predecessor",
    );

    let listed = gql::list_jobs(&client, admin, json!({ "runnerTypes": [runner_type] })).await;
    let predecessor_row = row_for(&listed, predecessor_id);
    let successor_row = row_for(&listed, successor_id);
    assert_eq!(
        predecessor_row["job"]["successorJobId"],
        json!(successor_id.to_string())
    );
    assert_eq!(
        successor_row["job"]["predecessorJobId"],
        json!(predecessor_id.to_string())
    );

    let by_source = gql::job_by_source(&client, admin, "projects", entity_id).await;
    assert_eq!(
        by_source["job"]["id"],
        json!(successor_id.to_string()),
        "the source lookup answers with the single non-terminal job, successor included",
    );

    let blocked_delete = gql::delete_job(&client, admin, predecessor_id).await;
    verdict::expect_code_shaped(
        &blocked_delete,
        "deleting a predecessor with a live successor",
    );
    gql::assert_blocked(
        &gql::job_view(&client, admin, predecessor_id).await,
        wire::ACTION_DELETE,
    );

    let absorbed = gql::manual_retry_job(
        &client,
        admin,
        intervention_id,
        predecessor_id,
        successor_id,
        failed_resolution_id,
    )
    .await;
    verdict::expect_ack(&absorbed, "an identical redelivery of the intervention");
    assert_eq!(
        gql::job(&client, admin, successor_id).await["attemptCount"],
        json!(1),
        "an absorbed intervention creates no second successor and no second run",
    );

    for (intervention, successor_candidate, resolution, what) in [
        (
            intervention_id,
            Uuid::now_v7(),
            failed_resolution_id,
            "reusing an intervention id for another successor",
        ),
        (
            Uuid::now_v7(),
            Uuid::now_v7(),
            Uuid::now_v7(),
            "a stale failed resolution",
        ),
        (
            Uuid::now_v7(),
            Uuid::now_v7(),
            failed_resolution_id,
            "a predecessor that already has a non-terminal successor",
        ),
    ] {
        let refused = gql::manual_retry_job(
            &client,
            admin,
            intervention,
            predecessor_id,
            successor_candidate,
            resolution,
        )
        .await;
        verdict::expect_code_shaped(&refused, what);
        let untouched = gql::job(&client, admin, predecessor_id).await;
        assert_eq!(
            untouched["manualRetry"]["id"],
            json!(intervention_id.to_string())
        );
    }

    let completed = JobDeclaration::new(&runner_type);
    let completed_id = completed.job_id;
    producer.declare(&completed).await;
    let completed_trigger = instance.next_trigger(LONG).await;
    instance.complete_run(&completed_trigger).await;
    producer.finish(completed_id, Uuid::now_v7()).await;
    gql::wait_for_status(&client, admin, completed_id, "COMPLETED", LONG).await;
    let completed_view = gql::job_view(&client, admin, completed_id).await;
    gql::assert_blocked(&completed_view, wire::ACTION_MANUAL_RETRY);
    let refused = gql::manual_retry_job(
        &client,
        admin,
        Uuid::now_v7(),
        completed_id,
        Uuid::now_v7(),
        Uuid::parse_str(
            completed_view["job"]["resolution"]["id"]
                .as_str()
                .expect("a completed job carries its resolution id"),
        )
        .expect("a resolution id is a UUID"),
    )
    .await;
    verdict::expect_code_shaped(&refused, "retrying a job that did not fail");

    events.stop().await;
    fixture.shutdown().await;
}

fn row_for(rows: &[serde_json::Value], job_id: Uuid) -> serde_json::Value {
    rows.iter()
        .find(|row| row["job"]["id"] == json!(job_id.to_string()))
        .cloned()
        .unwrap_or_else(|| panic!("job {job_id} must appear in the listing: {rows:?}"))
}
