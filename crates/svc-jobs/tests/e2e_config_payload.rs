mod support;

use serde_json::{Value, json};
use support::LONG;
use support::fixture::JobsFixture;
use support::lineage::live_runner;
use support::producer::{JobDeclaration, Producer};
use support::restart::retry_manually;
use support::runner;

const VIEW: &str = r#"
query($id: UUID!) {
  jobsJob(id: $id) {
    job { config payload { form message attachments { name contentType content } service documentId } }
  }
}
"#;

fn with_payload(declaration: &JobDeclaration, payload: Option<Value>) -> Value {
    let mut body = declaration.payload("projects");
    if let Some(payload) = payload {
        body["payload"] = payload;
    }
    body
}

#[tokio::test]
async fn config_and_payload_travel_apart_and_reach_every_successor_unchanged() {
    let fixture = JobsFixture::start().await;
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut x = live_runner(&fixture, "config-x").await;
    let inline = json!({ "form": "INLINE", "message": "summarise",
        "attachments": [{ "name": "a.txt", "content_type": "text/plain", "content": "aGk=" }] });

    let both = JobDeclaration::new(&x.runner_type)
        .with_config(json!({ "model": "m", "a": 1 }))
        .with_max_attempts(1);
    producer
        .declare_payload(with_payload(&both, Some(inline.clone())))
        .await;
    let trigger = x.next_trigger(LONG).await;
    assert_eq!(runner::job_id(&trigger), both.job_id);
    assert_eq!(
        trigger["config"],
        json!({ "model": "m", "a": 1 }),
        "the config reaches the runner as the same JSON value, not one field reshaped or dropped"
    );
    assert_eq!(trigger["payload"], inline, "the payload as given");
    let view = fixture
        .gql()
        .query(
            fixture.admin(),
            VIEW,
            json!({ "id": both.job_id.to_string() }),
        )
        .await;
    assert_eq!(
        view["data"]["jobsJob"]["job"]["config"],
        json!({ "model": "m", "a": 1 })
    );
    assert_eq!(
        view["data"]["jobsJob"]["job"]["payload"]["message"],
        "summarise"
    );
    assert_eq!(
        view["data"]["jobsJob"]["job"]["payload"]["attachments"][0]["content"],
        "aGk="
    );

    x.start_applied(&fixture, &trigger).await;
    x.fail_run(&trigger, "PERMANENT", "provider_refused", None)
        .await;
    let successor = retry_manually(&fixture, both.job_id).await;
    let retried = x.next_trigger(LONG).await;
    assert_eq!(runner::job_id(&retried), successor);
    assert_eq!(
        retried["config"], trigger["config"],
        "a manual-retry successor carries the config"
    );
    assert_eq!(retried["payload"], trigger["payload"], "and the payload");

    let config_only =
        JobDeclaration::new(&x.runner_type).with_config(json!({ "prompt": "the work text" }));
    producer
        .declare_payload(with_payload(&config_only, None))
        .await;
    let only = x.next_trigger(LONG).await;
    assert_eq!(runner::job_id(&only), config_only.job_id);
    assert!(
        only.get("payload").is_none_or(Value::is_null),
        "no payload: {only}"
    );

    let reference = json!({ "form": "REFERENCE", "service": "documents", "document_id": "doc-that-does-not-exist" });
    let referenced = JobDeclaration::new(&x.runner_type);
    producer
        .declare_payload(with_payload(&referenced, Some(reference.clone())))
        .await;
    let referred = x.next_trigger(LONG).await;
    assert_eq!(runner::job_id(&referred), referenced.job_id);
    assert_eq!(
        referred["payload"], reference,
        "a reference reaches the runner unchanged, unread"
    );

    for done in [&retried, &only, &referred] {
        x.start_applied(&fixture, done).await;
        x.complete_run(done).await;
    }
    let odd = json!([1, { "x": null, "nested": { "deep": [true, 2.5] } }, "free"]);
    let schemaless = JobDeclaration::new(&x.runner_type).with_config(odd.clone());
    producer.declare_v2(&schemaless).await;
    let free = x.next_trigger(LONG).await;
    assert_eq!(
        runner::job_id(&free),
        schemaless.job_id,
        "Jobs never validates the config schema"
    );
    assert_eq!(free["config"], odd);
    fixture.shutdown().await;
}
