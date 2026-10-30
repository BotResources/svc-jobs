mod support;

use br_test_harness::PassportBuilder;
use serde_json::json;
use support::events::EventLog;
use support::fixture::JobsFixture;
use support::gesture;
use support::producer::{JobDeclaration, Producer};
use support::{LONG, wire};
use uuid::Uuid;

#[tokio::test]
async fn a_creation_naming_a_kept_payload_is_accepted_only_from_the_actor_that_uploaded_it() {
    let fixture = JobsFixture::start().await;
    let answers = EventLog::open_v2(fixture.fabric()).await;
    let producer = Producer::new(fixture.fabric(), "projects");
    let gql = fixture.gql();
    let uploader = PassportBuilder::new()
        .user_id(Uuid::now_v7())
        .claim("scopes", vec!["jobs:runner"])
        .build_service();
    let runner_type = wire::unique_runner_type("kept-creation");

    let never_uploaded = JobDeclaration::new(&runner_type);
    let mut body = never_uploaded.payload("projects");
    body["payload"] = json!({ "form": "KEPT" });
    producer.declare_payload(body).await;
    let refused = answers
        .expect_one(wire::FACT_CREATION_REJECTED, never_uploaded.job_id, LONG)
        .await;
    assert_eq!(refused.payload()["reason_code"], "payload_not_uploaded");

    let foreign = JobDeclaration::new(&runner_type);
    let stored =
        gesture::upload_payload(&gql, &uploader, foreign.job_id, "their brief", None).await;
    gesture::accepted(&stored, "jobsUploadPayload");
    let mut body = foreign.payload("projects");
    body["payload"] = json!({ "form": "KEPT" });
    producer.declare_payload(body).await;
    let refused = answers
        .expect_one(wire::FACT_CREATION_REJECTED, foreign.job_id, LONG)
        .await;
    assert_eq!(
        refused.payload()["reason_code"],
        "payload_uploader_mismatch"
    );
    fixture.shutdown().await;
}
