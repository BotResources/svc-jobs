mod support;

use serde_json::json;
use support::fixture::JobsFixture;
use support::producer::{JobDeclaration, Producer};
use support::runner;
use support::surface::{self, Agent};
use support::tree::{call_child, pair};
use support::{LONG, gql, wire};
use uuid::Uuid;

#[tokio::test]
async fn a_failed_run_cancels_what_its_finished_child_left_running_below_it() {
    let fixture = JobsFixture::start().await;
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut pair = pair(&fixture, "cascade-terminal").await;
    let nats = fixture.nats();
    let root = JobDeclaration::new(&pair.parent.runner_type).with_max_attempts(1);
    producer.declare_v2(&root).await;
    let parent_trigger = pair.parent.next_trigger(LONG).await;
    pair.parent.start_applied(&fixture, &parent_trigger).await;
    let parent_run = runner::run_id(&parent_trigger);

    let called = call_child(&fixture, &mut pair, root.job_id, parent_run).await;
    pair.child.start_applied(&fixture, &called).await;
    let child_job = runner::job_id(&called);
    let child_run = runner::run_id(&called);
    let caller = Agent::with_inbox();
    let caller_address = caller.agent_id;
    surface::register(nats, &pair.child, child_run, &[caller]).await;

    let grandchild = Uuid::now_v7();
    producer
        .send_v3(
            wire::VERB_CREATE,
            json!({
                "job_id": grandchild.to_string(),
                "runner_type": wire::unique_runner_type("cascade-leaf"),
                "producer": "projects",
                "parent_job_id": child_job.to_string(),
                "mode": "BACKGROUND",
                "caller_address": caller_address.to_string(),
                "acting_run_id": child_run.to_string(),
                "acting_instance_key": pair.child.instance_key,
            }),
        )
        .await;
    gql::wait_for_status(&fixture.gql(), fixture.admin(), grandchild, "PENDING", LONG).await;

    pair.child.complete_run(&called).await;
    producer
        .send_v3(
            wire::VERB_FINISH,
            json!({
                "job_id": child_job.to_string(),
                "acting_run_id": parent_run.to_string(),
                "acting_instance_key": pair.parent.instance_key,
            }),
        )
        .await;
    gql::wait_for_status(
        &fixture.gql(),
        fixture.admin(),
        child_job,
        "COMPLETED",
        LONG,
    )
    .await;
    assert_eq!(
        gql::status_of(&fixture.gql(), fixture.admin(), grandchild).await,
        "PENDING",
        "finishing a Job leaves the Jobs below it alone"
    );

    pair.parent
        .fail_run(&parent_trigger, "PERMANENT", "provider_refused", None)
        .await;

    gql::wait_for_status(
        &fixture.gql(),
        fixture.admin(),
        grandchild,
        "CANCELLED",
        LONG,
    )
    .await;
    fixture.shutdown().await;
}
