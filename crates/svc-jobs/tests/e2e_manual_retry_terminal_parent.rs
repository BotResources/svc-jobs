mod support;

use support::events::EventLog;
use support::fixture::JobsFixture;
use support::producer::{JobDeclaration, Producer};
use support::runner::FakeRunner;
use support::{LONG, QUIET, gql, wire};
use uuid::Uuid;

#[tokio::test]
async fn a_manual_retry_under_a_terminal_parent_is_refused_with_a_reason() {
    let fixture = JobsFixture::start().await;
    let events = EventLog::open(fixture.fabric()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let parent_type = wire::unique_runner_type("retry-parent");
    let child_type = wire::unique_runner_type("retry-child");
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut worker = FakeRunner::new(fixture.nats(), &child_type, "worker-a");
    worker.connect().await;

    let parent = JobDeclaration::new(&parent_type);
    let parent_id = parent.job_id;
    producer.declare(&parent).await;
    events.expect_one(wire::FACT_QUEUED, parent_id, LONG).await;

    let child = JobDeclaration::new(&child_type)
        .with_parent(parent_id)
        .with_max_attempts(1);
    let child_id = child.job_id;
    producer.declare(&child).await;
    let trigger = worker.next_trigger(LONG).await;
    worker.start_run(&trigger).await;
    worker
        .fail_run(&trigger, "PERMANENT", "unsupported_format", None)
        .await;
    gql::wait_for_status(&client, admin, child_id, "FAILED", LONG).await;
    let failed = gql::job_view(&client, admin, child_id).await;
    let failed_resolution = Uuid::parse_str(
        failed["job"]["resolution"]["id"]
            .as_str()
            .expect("a failed job names its resolution"),
    )
    .unwrap();

    producer.fail(parent_id, None).await;
    gql::wait_for_status(&client, admin, parent_id, "FAILED", LONG).await;
    let refused = gql::manual_retry_job(
        &client,
        admin,
        Uuid::now_v7(),
        child_id,
        Uuid::now_v7(),
        failed_resolution,
    )
    .await;
    assert_eq!(
        gql::mutation_error_reason(&refused, "a retry under a terminal parent"),
        "parent_job_terminal"
    );
    worker.expect_no_trigger(QUIET).await;
    fixture.shutdown().await;
}
