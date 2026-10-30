mod support;

use br_test_harness::PassportBuilder;
use serde_json::json;
use support::fixture::{JobsFixture, runner_access};
use support::gesture::{self, Work};
use support::producer::{JobDeclaration, Producer};
use support::runner::FakeRunner;
use support::{LONG, dispatch, gql, wire};

fn inline_body(declaration: &JobDeclaration) -> serde_json::Value {
    let mut body = declaration.payload("projects");
    body["payload"] = json!({ "form": "INLINE", "message": "the inline brief", "attachments": [] });
    body
}

#[tokio::test]
async fn a_job_created_with_an_inline_payload_takes_no_upload_afterwards() {
    let fixture = JobsFixture::start().await;
    let client = fixture.gql();
    let producer = Producer::new(fixture.fabric(), "projects");
    let uploader = PassportBuilder::new()
        .user_id(producer.account_id)
        .claim("scopes", vec!["jobs:runner"])
        .build_service();
    let runner_type = wire::unique_runner_type("form-after-create");
    let mut instance = FakeRunner::new(fixture.nats(), &runner_type, "instance-a");
    instance.connect().await;
    dispatch::await_live_instances(&client, fixture.admin(), &runner_type, 1, LONG).await;
    let declaration = JobDeclaration::new(&runner_type);
    producer.declare_payload(inline_body(&declaration)).await;
    let trigger = instance.next_trigger(LONG).await;
    instance.start_applied(&fixture, &trigger).await;

    let refused =
        gesture::upload_payload(&client, &uploader, declaration.job_id, "an intruder", None).await;
    assert_eq!(
        gesture::refusal(&refused, "jobsUploadPayload"),
        "payload_already_uploaded"
    );

    let work = Work::of(&instance, &trigger);
    let fetched = gesture::payload(&client, &runner_access(), &work).await;
    assert_eq!(
        gesture::refusal(&fetched, "jobsPayload"),
        "no_kept_payload",
        "{fetched}"
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_job_without_any_payload_takes_no_upload_afterwards() {
    let fixture = JobsFixture::start().await;
    let client = fixture.gql();
    let producer = Producer::new(fixture.fabric(), "projects");
    let uploader = PassportBuilder::new()
        .user_id(producer.account_id)
        .claim("scopes", vec!["jobs:runner"])
        .build_service();
    let declaration = JobDeclaration::new(&wire::unique_runner_type("form-none"));
    producer.declare_v2(&declaration).await;
    gql::wait_for_status(
        &client,
        fixture.admin(),
        declaration.job_id,
        "PENDING",
        LONG,
    )
    .await;

    let refused =
        gesture::upload_payload(&client, &uploader, declaration.job_id, "an intruder", None).await;
    assert_eq!(
        gesture::refusal(&refused, "jobsUploadPayload"),
        "payload_already_uploaded"
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_payload_uploaded_for_a_job_id_later_created_with_another_form_is_never_served() {
    let fixture = JobsFixture::start().await;
    let client = fixture.gql();
    let producer = Producer::new(fixture.fabric(), "projects");
    let uploader = PassportBuilder::new()
        .user_id(producer.account_id)
        .claim("scopes", vec!["jobs:runner"])
        .build_service();
    let runner_type = wire::unique_runner_type("form-before-create");
    let mut instance = FakeRunner::new(fixture.nats(), &runner_type, "instance-a");
    instance.connect().await;
    dispatch::await_live_instances(&client, fixture.admin(), &runner_type, 1, LONG).await;
    let declaration = JobDeclaration::new(&runner_type);
    let stored =
        gesture::upload_payload(&client, &uploader, declaration.job_id, "stray upload", None).await;
    gesture::accepted(&stored, "jobsUploadPayload");

    producer.declare_payload(inline_body(&declaration)).await;
    let trigger = instance.next_trigger(LONG).await;
    instance.start_applied(&fixture, &trigger).await;
    let work = Work::of(&instance, &trigger);

    let fetched = gesture::payload(&client, &runner_access(), &work).await;
    assert_eq!(
        gesture::refusal(&fetched, "jobsPayload"),
        "no_kept_payload",
        "{fetched}"
    );
    fixture.shutdown().await;
}
