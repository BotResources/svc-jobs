mod support;

use br_test_harness::SseSubscription;
use serde_json::json;
use support::events::EventLog;
use support::fixture::JobsFixture;
use support::producer::{JobDeclaration, Producer};
use support::runner::{self, FakeRunner};
use support::{LONG, QUIET, SHORT, docs, gql, stream, wire};
use uuid::Uuid;

const JOB_CHANGED: &str = "jobsJobChanged";

#[tokio::test]
async fn a_child_failure_informs_its_parent_without_deciding_the_parents_fate() {
    let fixture = JobsFixture::start().await;
    let events = EventLog::open(fixture.fabric()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let parent_type = wire::unique_runner_type("orchestrator");
    let child_type = wire::unique_runner_type("worker");
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut orchestrator = FakeRunner::new(fixture.nats(), &parent_type, "orchestrator-a");
    let mut worker = FakeRunner::new(fixture.nats(), &child_type, "worker-a");
    orchestrator.connect().await;
    worker.connect().await;

    let parent = JobDeclaration::new(&parent_type);
    let parent_id = parent.job_id;
    producer.declare(&parent).await;
    events.expect_one(wire::FACT_QUEUED, parent_id, LONG).await;
    let parent_trigger = orchestrator.next_trigger(LONG).await;
    orchestrator.start_run(&parent_trigger).await;
    gql::wait_for_status(&client, admin, parent_id, "IN_PROGRESS", LONG).await;

    let owner = Producer::new(fixture.fabric(), "jobs");
    let child = JobDeclaration::new(&child_type).with_parent(parent_id);
    let child_id = child.job_id;
    owner.declare(&child).await;
    events.expect_one(wire::FACT_QUEUED, child_id, LONG).await;

    let mut parent_watch = SseSubscription::open(
        fixture.url(),
        admin,
        &docs::job_changed_subscription(parent_id),
    )
    .await;
    stream::snapshot(&mut parent_watch, JOB_CHANGED, SHORT).await;

    let child_trigger = worker.next_trigger(LONG).await;
    let child_run = runner::run_id(&child_trigger);
    worker.start_run(&child_trigger).await;
    worker
        .fail_run(&child_trigger, "PERMANENT", "provider_refused", None)
        .await;

    gql::wait_for_status(&client, admin, child_id, "FAILED", LONG).await;
    let failure = events.expect_one(wire::FACT_FAILED, child_id, LONG).await;
    let payload = failure.payload();
    assert_eq!(payload["failure_cause"], json!("TERMINAL_RUN_FAILURE"));
    assert_eq!(
        payload["failure_report"]["reason_code"],
        json!("provider_refused"),
        "the run's report is escalated to the owner as it was reported: {payload}",
    );
    assert_eq!(payload["failure_report"]["kind"], json!("PERMANENT"));

    let failed_child = gql::job(&client, admin, child_id).await;
    assert_eq!(failed_child["parentJobId"], json!(parent_id.to_string()));
    assert_eq!(
        failed_child["resolution"]["failureCause"],
        json!("TERMINAL_RUN_FAILURE")
    );
    assert_eq!(
        failed_child["resolution"]["causedByRunId"],
        json!(child_run.to_string())
    );
    let report = gql::run_by_id(&failed_child, child_run)["failureReport"].clone();
    assert_eq!(report["reasonCode"], json!("provider_refused"));
    assert_eq!(report["kind"], json!("PERMANENT"));
    assert!(
        failed_child["nextAttemptAt"].is_null(),
        "a permanent failure ends automatic retry immediately: {failed_child}",
    );
    worker.expect_no_trigger(QUIET).await;

    let parent_view = gql::job_view(&client, admin, parent_id).await;
    assert_eq!(
        parent_view["job"]["status"],
        json!("IN_PROGRESS"),
        "a child's failure does not change its parent",
    );
    assert!(parent_view["job"]["resolution"].is_null());
    gql::assert_allowed(&parent_view, wire::ACTION_CANCEL);
    events
        .expect_none(wire::FACT_FAILED, parent_id, QUIET)
        .await;
    stream::expect_no_delta(&mut parent_watch, JOB_CHANGED, "JobsJobFailedEvent", QUIET).await;

    let child_in_tree = gql::child_of(&parent_view["job"], child_id);
    assert_eq!(child_in_tree["job"]["status"], json!("FAILED"));
    gql::assert_blocked(&child_in_tree, wire::ACTION_CANCEL);
    gql::assert_allowed(&child_in_tree, wire::ACTION_MANUAL_RETRY);

    let owner_resolution = Uuid::now_v7();
    producer.fail(parent_id, owner_resolution).await;
    gql::wait_for_status(&client, admin, parent_id, "FAILED", LONG).await;
    let parent_failure = events.expect_one(wire::FACT_FAILED, parent_id, LONG).await;
    assert_eq!(
        parent_failure.payload()["failure_cause"],
        json!("DECLARED_BY_OWNER"),
        "the owner decided the parent's fate, the child never did",
    );
    stream::await_delta(&mut parent_watch, JOB_CHANGED, "JobsJobFailedEvent", LONG).await;

    let decided = gql::job(&client, admin, parent_id).await;
    assert_eq!(
        decided["resolution"]["id"],
        json!(owner_resolution.to_string())
    );
    assert!(
        decided["resolution"]["causedByRunId"].is_null(),
        "an owner-declared failure is not caused by a run: {decided}",
    );

    let late = Uuid::now_v7();
    producer.fail(parent_id, late).await;
    events
        .expect_exactly(wire::FACT_FAILED, parent_id, 1, QUIET)
        .await;
    assert_eq!(
        gql::job(&client, admin, parent_id).await["resolution"]["id"],
        json!(owner_resolution.to_string()),
        "a terminal resolution is immutable",
    );

    events.stop().await;
    fixture.shutdown().await;
}
