mod support;

use serde_json::{Value, json};
use support::fixture::JobsFixture;
use support::producer::{JobDeclaration, Producer};
use support::runner;
use support::tree::{call_child, pair};
use support::{LONG, gql, wire};
use uuid::Uuid;

const ORIGIN: &str = r#"
query($id: UUID!) {
  jobsJob(id: $id) {
    job {
      id parentJobId initialJobId
      passport { subjectKind subjectId document }
      requester { kind serviceId runId }
      call { mode callerAddress }
      payload { form message attachments { name contentType content } service documentId }
    }
  }
}
"#;

async fn origin(fixture: &JobsFixture, job_id: Uuid) -> Value {
    let response = fixture
        .gql()
        .query(fixture.admin(), ORIGIN, json!({ "id": job_id.to_string() }))
        .await;
    assert!(response["errors"].is_null(), "{response}");
    response["data"]["jobsJob"]["job"].clone()
}

#[tokio::test]
async fn an_administrator_reads_the_passport_requester_mode_payload_and_initial_job_of_a_job() {
    let fixture = JobsFixture::start().await;
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut pair = pair(&fixture, "view-origin").await;

    let root = JobDeclaration::new(&pair.parent.runner_type).with_max_attempts(1);
    let mut body = root.payload("projects");
    body["payload"] = json!({
        "form": "INLINE",
        "message": "summarise the file",
        "attachments": [
            { "name": "notes.txt", "content_type": "text/plain", "content": "aGVsbG8=" }
        ],
    });
    producer.declare_payload(body).await;
    let trigger = pair.parent.next_trigger(LONG).await;
    pair.parent.start_applied(&fixture, &trigger).await;
    let parent_run = runner::run_id(&trigger);

    let root_view = origin(&fixture, root.job_id).await;
    assert_eq!(root_view["initialJobId"], root.job_id.to_string());
    assert_eq!(root_view["parentJobId"], Value::Null);
    assert_eq!(root_view["passport"]["subjectKind"], "SERVICE");
    assert_eq!(
        root_view["passport"]["subjectId"],
        Uuid::from_u128(0x7e57).to_string()
    );
    assert!(root_view["passport"]["document"].is_object());
    assert_eq!(root_view["requester"]["kind"], "SERVICE");
    assert_eq!(
        root_view["requester"]["serviceId"],
        producer.account_id.to_string()
    );
    assert_eq!(root_view["payload"]["form"], "INLINE");
    assert_eq!(root_view["payload"]["message"], "summarise the file");
    assert_eq!(
        root_view["payload"]["attachments"],
        json!([{ "name": "notes.txt", "contentType": "text/plain", "content": "aGVsbG8=" }])
    );

    let called = call_child(&fixture, &mut pair, root.job_id, parent_run).await;
    let child_view = origin(&fixture, runner::job_id(&called)).await;
    assert_eq!(child_view["parentJobId"], root.job_id.to_string());
    assert_eq!(child_view["initialJobId"], root.job_id.to_string());
    assert_eq!(child_view["requester"]["kind"], "RUN");
    assert_eq!(child_view["requester"]["runId"], parent_run.to_string());
    assert_eq!(child_view["call"]["mode"], "SYNC");
    assert_eq!(child_view["call"]["callerAddress"], Value::Null);
    assert_eq!(child_view["passport"], root_view["passport"]);
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_payload_kept_elsewhere_shows_its_form_and_reference_only() {
    let fixture = JobsFixture::start().await;
    let producer = Producer::new(fixture.fabric(), "projects");
    let runner_type = wire::unique_runner_type("view-reference");
    let declaration = JobDeclaration::new(&runner_type);
    let mut body = declaration.payload("projects");
    body["payload"] = json!({
        "form": "REFERENCE",
        "service": "documents",
        "document_id": "doc-42",
    });
    producer.declare_payload(body).await;
    gql::wait_for_status(
        &fixture.gql(),
        fixture.admin(),
        declaration.job_id,
        "PENDING",
        LONG,
    )
    .await;

    let view = origin(&fixture, declaration.job_id).await;
    assert_eq!(view["payload"]["form"], "REFERENCE");
    assert_eq!(view["payload"]["service"], "documents");
    assert_eq!(view["payload"]["documentId"], "doc-42");
    assert_eq!(view["payload"]["message"], Value::Null);
    assert_eq!(view["payload"]["attachments"], json!([]));
    fixture.shutdown().await;
}
