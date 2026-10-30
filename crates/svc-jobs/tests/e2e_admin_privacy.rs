mod support;

use br_test_harness::{PassportBuilder, verdict};
use serde_json::json;
use support::db::Durable;
use support::directory::{listed, tree_agents};
use support::fixture::{JobsFixture, runner_access};
use support::gesture::{self, Mail, Work};
use support::producer::{JobDeclaration, Producer};
use support::restart::{result_for, upload_result};
use support::runner;
use support::surface::{self, Agent};
use support::tree::{call_child, pair};
use support::{LONG, docs};
use uuid::Uuid;

const PAYLOAD_VIEW: &str = r#"
query($id: UUID!) {
  jobsJob(id: $id) {
    job { id payload { form message attachments { name contentType content } service documentId } }
  }
}
"#;

#[tokio::test]
async fn an_administrator_sees_the_form_of_a_kept_payload_and_the_directory_but_no_content_result_or_mail()
 {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let gql = fixture.gql();
    let admin = fixture.admin();
    let producer = Producer::new(fixture.fabric(), "projects");
    let uploader = PassportBuilder::new()
        .user_id(producer.account_id)
        .claim("scopes", vec!["jobs:runner"])
        .build_service();
    let mut pair = pair(&fixture, "admin-privacy").await;
    let root = JobDeclaration::new(&pair.parent.runner_type).with_max_attempts(1);
    let stored = gesture::upload_payload(&gql, &uploader, root.job_id, "secret brief", None).await;
    gesture::accepted(&stored, "jobsUploadPayload");
    let mut body = root.payload("projects");
    body["payload"] = json!({ "form": "KEPT" });
    producer.declare_payload(body).await;
    let root_trigger = pair.parent.next_trigger(LONG).await;
    pair.parent.start_applied(&fixture, &root_trigger).await;
    let root_work = Work::of(&pair.parent, &root_trigger);
    let (sender, receiver) = (Agent::with_inbox(), Agent::with_inbox());
    let (sender_id, receiver_id) = (sender.agent_id, receiver.agent_id);
    surface::register(
        fixture.nats(),
        &pair.parent,
        root_work.run_id,
        &[sender, receiver],
    )
    .await;
    let mail = Mail::new(sender_id, &[receiver_id]).with_body("secret mail body");
    gesture::accepted(
        &gesture::send_mail(&gql, &runner_access(), &root_work, &mail).await,
        "jobsSendMail",
    );
    let child = call_child(&fixture, &mut pair, root.job_id, root_work.run_id).await;
    pair.child.start_applied(&fixture, &child).await;
    let child_work = Work::of(&pair.child, &child);
    upload_result(&fixture, &child_work, "secret answer").await;
    let result = result_for(&durable, child_work.job_id).await;

    let view = gql
        .query(
            admin,
            PAYLOAD_VIEW,
            json!({ "id": root.job_id.to_string() }),
        )
        .await;
    assert!(view["errors"].is_null(), "{view}");
    let payload = &view["data"]["jobsJob"]["job"]["payload"];
    assert_eq!(
        payload["form"], "KEPT",
        "the view shows the form of the payload"
    );
    assert!(
        payload["message"].is_null(),
        "and not its content: {payload}"
    );
    assert_eq!(payload["attachments"], json!([]));
    for job in [root.job_id, runner::job_id(&child)] {
        let detail = gql
            .query(admin, docs::JOB_DETAIL, json!({ "id": job.to_string() }))
            .await;
        let text = format!("{view}{detail}");
        for secret in ["secret brief", "secret mail body", "secret answer"] {
            assert!(
                !text.contains(secret),
                "the administrator view shows no {secret}"
            );
        }
    }

    let any = Uuid::now_v7().to_string();
    for (document, variables) in [
        (gesture::PAYLOAD, json!({ "work": root_work.wire() })),
        (
            gesture::READ_RESULT,
            json!({ "input": { "work": root_work.wire(), "resultId": result.to_string(), "keep": true } }),
        ),
        (
            gesture::MAIL,
            json!({ "work": root_work.wire(), "mailId": mail.mail_id.to_string(), "address": receiver_id.to_string() }),
        ),
        (gesture::UNREAD, json!({ "work": root_work.wire() })),
        (
            gesture::ATTACHMENT,
            json!({ "work": root_work.wire(), "mailId": mail.mail_id.to_string(), "attachmentId": any }),
        ),
        (gesture::SAVED_STATE, json!({ "work": root_work.wire() })),
    ] {
        let refused = gesture::call(&gql, admin, document, variables).await;
        verdict::expect_code_shaped(&refused, "no administrator gesture reads runner content");
        let text = refused.to_string();
        for secret in ["secret brief", "secret mail body", "secret answer"] {
            assert!(!text.contains(secret), "{text}");
        }
    }
    let still_there = gesture::call(
        &gql,
        &runner_access(),
        gesture::READ_RESULT,
        json!({ "input": { "work": root_work.wire(), "resultId": result.to_string(), "keep": false } }),
    )
    .await;
    assert_eq!(
        gesture::field(&still_there, "jobsReadResult")["content"]["message"],
        "secret answer",
        "the administrator's attempt read nothing away"
    );

    let directory = listed(&tree_agents(&fixture, admin, root.job_id).await);
    let mut seen: Vec<String> = directory
        .iter()
        .map(|agent| agent["agentId"].as_str().unwrap_or_default().to_owned())
        .collect();
    seen.sort();
    let mut expected = vec![sender_id.to_string(), receiver_id.to_string()];
    expected.sort();
    assert_eq!(
        seen, expected,
        "the administrator sees the agent directory of the Tree"
    );
    durable.close().await;
    fixture.shutdown().await;
}
