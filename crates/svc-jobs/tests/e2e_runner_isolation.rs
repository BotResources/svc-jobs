mod support;

use br_test_harness::verdict;
use serde_json::{Value, json};
use support::db::Durable;
use support::fixture::{JobsFixture, machine_caller, runner_access};
use support::gesture::{self, Mail, Work};
use support::restart::{read_result, result_for, running_child, upload_result};
use support::surface::{self, Agent};
use uuid::Uuid;

const SAVED_OF_RUN: &str = "SELECT count(*) FROM saved_states WHERE run_id = $1";
const RESULTS_OF_JOB: &str = "SELECT count(*) FROM job_results WHERE job_id = $1";

fn reason_of(response: &Value, name: &str) -> String {
    gesture::refusal(response, name)
}

async fn attachment(fixture: &JobsFixture, work: &Work, mail: &Mail, id: Uuid) -> Value {
    gesture::call(
        &fixture.gql(),
        &runner_access(),
        gesture::ATTACHMENT,
        json!({ "work": work.wire(), "mailId": mail.mail_id.to_string(), "attachmentId": id.to_string() }),
    )
    .await
}

async fn upload_answer(fixture: &JobsFixture, work: &Work) -> Value {
    gesture::call(
        &fixture.gql(),
        &runner_access(),
        gesture::UPLOAD_RESULT,
        json!({ "input": { "work": work.wire(), "answer": gesture::content("hijack", &[]) } }),
    )
    .await
}

async fn save(fixture: &JobsFixture, work: &Work) -> Value {
    gesture::call(
        &fixture.gql(),
        &runner_access(),
        gesture::SAVE_STATE,
        json!({ "input": { "work": work.wire(), "state": { "stolen": true } } }),
    )
    .await
}

#[tokio::test]
async fn a_runner_uses_only_its_own_runs_addresses_and_jobs() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let gql = fixture.gql();
    let runner = runner_access();
    let scene = running_child(&fixture, "runner-isolation").await;
    let x = scene.parent_work.clone();
    let y = scene.child_work.clone();
    let file = Uuid::now_v7();
    let mail = Mail::new(scene.sender, &[scene.peer]).with_attachment(file, "x.txt");
    gesture::accepted(
        &gesture::upload_attachment(&gql, &runner, &x, &mail, file, "x.txt", "secret").await,
        "jobsUploadAttachment",
    );
    gesture::accepted(
        &gesture::send_mail(&gql, &runner, &x, &mail).await,
        "jobsSendMail",
    );
    upload_result(&fixture, &y, "the answer for X").await;
    let result = result_for(&durable, y.job_id).await;
    let x_items = gesture::items(&gesture::unread(&gql, &runner, &x, Some(scene.peer)).await);
    let x_item = Uuid::parse_str(x_items[0]["itemId"].as_str().unwrap()).unwrap();
    let x_job = Work {
        job_id: x.job_id,
        ..y.clone()
    };
    let x_run = Work {
        run_id: x.run_id,
        ..y.clone()
    };

    let payload = gesture::payload(&gql, &runner, &x_job).await;
    assert_eq!(reason_of(&payload, "jobsPayload"), "job_not_of_run");
    let stranger_result = gql
        .query(
            &runner,
            gesture::READ_RESULT,
            json!({ "input": { "work": y.wire(), "resultId": result.to_string(), "keep": true } }),
        )
        .await;
    assert_eq!(
        reason_of(&stranger_result, "jobsReadResult"),
        "not_addressed_to_you"
    );
    let forged =
        gesture::send_mail(&gql, &runner, &y, &Mail::new(scene.peer, &[scene.sender])).await;
    assert_eq!(reason_of(&forged, "jobsSendMail"), "sender_not_of_run");
    let by_id = gesture::read_mail(&gql, &runner, &y, mail.mail_id, scene.peer).await;
    assert_eq!(reason_of(&by_id, "jobsMail"), "address_not_yours");
    let unread = gesture::unread(&gql, &runner, &y, Some(scene.peer)).await;
    assert_eq!(reason_of(&unread, "jobsUnread"), "address_not_yours");
    let fetched = attachment(&fixture, &y, &mail, file).await;
    assert_eq!(reason_of(&fetched, "jobsAttachment"), "mail_not_yours");
    let marked = gesture::mark_read(fixture.nats(), &scene.pair.child, y.run_id, &[x_item]).await;
    assert_eq!(surface::reason(&marked), "item_not_yours");
    assert_eq!(
        reason_of(&save(&fixture, &x_run).await, "jobsSaveState"),
        "job_not_of_run"
    );
    let registered = surface::register_as(
        fixture.nats(),
        &scene.pair.child.runner_type,
        &scene.pair.child.instance_key,
        x.run_id,
        &[Agent::with_inbox()],
    )
    .await;
    assert!(!surface::reason(&registered).is_empty(), "{registered}");
    assert_eq!(
        reason_of(&upload_answer(&fixture, &x_job).await, "jobsUploadResult"),
        "job_not_of_run"
    );

    durable
        .assert_count(SAVED_OF_RUN, x.run_id, 0, "no state was handed for X")
        .await;
    durable
        .assert_count(RESULTS_OF_JOB, x.job_id, 0, "no answer was uploaded for J")
        .await;
    assert_eq!(
        gesture::items(&gesture::unread(&gql, &runner, &x, Some(scene.peer)).await),
        x_items,
        "X's items are unchanged"
    );
    assert_eq!(
        surface::agent_ids(&surface::list(fixture.nats(), &scene.pair.parent, x.run_id).await)
            .len(),
        4,
        "no agent was registered for X's Run"
    );

    let elsewhere = y.as_instance("instance-elsewhere");
    let unknown_result = gql
        .query(
            &runner,
            gesture::READ_RESULT,
            json!({ "input": { "work": elsewhere.wire(), "resultId": Uuid::now_v7().to_string(), "keep": false } }),
        )
        .await;
    assert_eq!(
        reason_of(&unknown_result, "jobsReadResult"),
        "instance_mismatch",
        "refused first"
    );
    let foreign_job = Work {
        job_id: x.job_id,
        ..elsewhere.clone()
    };
    assert_eq!(
        reason_of(
            &gesture::payload(&gql, &runner, &foreign_job).await,
            "jobsPayload"
        ),
        "instance_mismatch"
    );
    assert_eq!(
        reason_of(&save(&fixture, &elsewhere).await, "jobsSaveState"),
        "instance_mismatch"
    );

    let unscoped = machine_caller();
    let any = Uuid::now_v7().to_string();
    for (document, variables) in [
        (gesture::PAYLOAD, json!({ "work": y.wire() })),
        (gesture::UNREAD, json!({ "work": y.wire() })),
        (gesture::SAVED_STATE, json!({ "work": y.wire() })),
        (
            gesture::MAIL,
            json!({ "work": y.wire(), "mailId": any, "address": any }),
        ),
        (
            gesture::ATTACHMENT,
            json!({ "work": y.wire(), "mailId": any, "attachmentId": any }),
        ),
        (
            gesture::SEND_MAIL,
            json!({ "input": Mail::new(scene.receiver, &[scene.bystander]).input(&y) }),
        ),
        (
            gesture::SAVE_STATE,
            json!({ "input": { "work": y.wire(), "state": {} } }),
        ),
        (
            gesture::UPLOAD_RESULT,
            json!({ "input": { "work": y.wire(), "answer": gesture::content("a", &[]) } }),
        ),
        (
            gesture::READ_RESULT,
            json!({ "input": { "work": x.wire(), "resultId": result.to_string(), "keep": true } }),
        ),
        (
            gesture::UPLOAD_PAYLOAD,
            json!({ "input": { "forJobId": any, "content": gesture::content("a", &[]) } }),
        ),
    ] {
        let refused = gesture::call(&gql, &unscoped, document, variables).await;
        verdict::expect_code_shaped(&refused, "a caller without the runner scope");
    }

    let own_result = read_result(&fixture, &x, result).await;
    assert_eq!(
        gesture::field(&own_result, "jobsReadResult")["success"],
        true,
        "{own_result}"
    );
    let own_mail = gesture::read_mail(&gql, &runner, &x, mail.mail_id, scene.peer).await;
    assert_eq!(gesture::field(&own_mail, "jobsMail")["success"], true);
    let own_file = attachment(&fixture, &x, &mail, file).await;
    assert_eq!(
        gesture::field(&own_file, "jobsAttachment")["attachment"]["content"],
        "secret"
    );
    gesture::accepted(&save(&fixture, &x).await, "jobsSaveState");
    let own_mark =
        gesture::mark_read(fixture.nats(), &scene.pair.parent, x.run_id, &[x_item]).await;
    assert_eq!(own_mark["accepted"], true, "{own_mark}");
    let own_register = surface::register(
        fixture.nats(),
        &scene.pair.parent,
        x.run_id,
        &[Agent::with_inbox()],
    )
    .await;
    assert_eq!(own_register["accepted"], true, "{own_register}");
    durable.close().await;
    fixture.shutdown().await;
}
