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
async fn an_administrator_cancels_a_job_tree_without_leaving_work_running_or_queued() {
    let fixture = JobsFixture::start().await;
    let events = EventLog::open(fixture.fabric()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let parent_type = wire::unique_runner_type("tree_root");
    let running_type = wire::unique_runner_type("tree_running");
    let queued_type = wire::unique_runner_type("tree_queued");
    let producer = Producer::new(fixture.fabric(), "projects");
    let owner = Producer::new(fixture.fabric(), "jobs");

    let mut root = FakeRunner::new(fixture.nats(), &parent_type, "root-a");
    let mut busy = FakeRunner::new(fixture.nats(), &running_type, "busy-a");
    let mut idle = FakeRunner::new(fixture.nats(), &queued_type, "idle-a");
    root.connect().await;
    busy.connect().await;
    idle.connect().await;

    let parent = JobDeclaration::new(&parent_type);
    let parent_id = parent.job_id;
    producer.declare(&parent).await;
    events.expect_one(wire::FACT_QUEUED, parent_id, LONG).await;
    let parent_trigger = root.next_trigger(LONG).await;
    let parent_run = runner::run_id(&parent_trigger);
    root.start_run(&parent_trigger).await;
    gql::wait_for_status(&client, admin, parent_id, "IN_PROGRESS", LONG).await;

    let running_child = JobDeclaration::new(&running_type).with_parent(parent_id);
    let running_child_id = running_child.job_id;
    owner.declare(&running_child).await;
    let running_trigger = busy.next_trigger(LONG).await;
    let running_child_run = runner::run_id(&running_trigger);
    busy.start_run(&running_trigger).await;
    gql::wait_for_status(&client, admin, running_child_id, "IN_PROGRESS", LONG).await;

    let queued_child = JobDeclaration::new(&queued_type).with_parent(parent_id);
    let queued_child_id = queued_child.job_id;
    owner.declare(&queued_child).await;
    events
        .expect_one(wire::FACT_QUEUED, queued_child_id, LONG)
        .await;
    gql::wait_for_attempts(&client, admin, queued_child_id, 1, LONG).await;
    let queued_child_run = Uuid::parse_str(
        gql::run_by_attempt(&gql::job(&client, admin, queued_child_id).await, 1)["id"]
            .as_str()
            .expect("a dispatched run carries its id"),
    )
    .expect("a run id is a UUID");

    let mut watch = SseSubscription::open(
        fixture.url(),
        admin,
        &docs::job_changed_subscription(parent_id),
    )
    .await;
    stream::snapshot(&mut watch, JOB_CHANGED, SHORT).await;

    let resolution_id = Uuid::now_v7();
    let ack = gql::cancel_job(&client, admin, resolution_id, parent_id).await;
    verdict::expect_ack(&ack, "an administrator cancels a non-terminal job");
    assert_eq!(ack["data"]["jobsCancelJob"], json!({ "success": true }));

    for job_id in [parent_id, running_child_id, queued_child_id] {
        gql::wait_for_status(&client, admin, job_id, "CANCELLED", LONG).await;
        let cancelled = events.expect_one(wire::FACT_CANCELLED, job_id, LONG).await;
        assert_eq!(
            cancelled.payload()["job_id"],
            json!(job_id.to_string()),
            "cancellation travels downward, one event per cancelled job under its own id",
        );
    }
    stream::await_delta(&mut watch, JOB_CHANGED, "JobsJobCancelledEvent", LONG).await;

    let parent_entry = root.await_cancel_entry(parent_run, LONG).await;
    assert_eq!(parent_entry["run_id"], json!(parent_run.to_string()));
    busy.await_cancel_entry(running_child_run, LONG).await;
    idle.expect_no_cancel_entry(queued_child_run, QUIET).await;

    let withdrawn = gql::run_by_attempt(&gql::job(&client, admin, queued_child_id).await, 1);
    assert_eq!(
        withdrawn["status"],
        json!("CANCELLED"),
        "queued work is withdrawn rather than left in line",
    );

    let cancelled_parent = gql::job_view(&client, admin, parent_id).await;
    assert_eq!(
        cancelled_parent["job"]["resolution"]["id"],
        json!(resolution_id.to_string())
    );
    gql::assert_blocked(&cancelled_parent, wire::ACTION_CANCEL);
    gql::assert_allowed(&cancelled_parent, wire::ACTION_DELETE);
    let cancelled_child = gql::child_of(&cancelled_parent["job"], running_child_id);
    gql::assert_blocked(&cancelled_child, wire::ACTION_CANCEL);

    busy.complete_run(&running_trigger).await;
    let withdrawn_trigger = idle.next_trigger(LONG).await;
    assert_eq!(runner::run_id(&withdrawn_trigger), queued_child_run);
    idle.start_run(&withdrawn_trigger).await;
    events
        .expect_none(wire::FACT_COMPLETED, running_child_id, QUIET)
        .await;
    events
        .expect_none(wire::FACT_STARTED, queued_child_id, QUIET)
        .await;

    let reconciled = gql::job(&client, admin, running_child_id).await;
    assert_eq!(reconciled["status"], json!("CANCELLED"));
    assert_ne!(
        gql::run_by_id(&reconciled, running_child_run)["status"],
        json!("COMPLETED"),
        "a late fact against a terminal job is acknowledged and discarded",
    );
    assert_eq!(
        gql::job(&client, admin, queued_child_id).await["attemptCount"],
        json!(1),
        "a discarded late fact adds no history",
    );

    assert_eq!(root.trigger_count().await, 1);
    assert_eq!(busy.trigger_count().await, 1);
    assert_eq!(idle.trigger_count().await, 1);

    let repeat = gql::cancel_job(&client, admin, Uuid::now_v7(), parent_id).await;
    verdict::expect_code_shaped(&repeat, "cancelling an already terminal job");
    events
        .expect_exactly(wire::FACT_CANCELLED, parent_id, 1, QUIET)
        .await;

    events.stop().await;
    fixture.shutdown().await;
}
