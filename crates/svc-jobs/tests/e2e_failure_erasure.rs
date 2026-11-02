mod support;

use serde_json::json;
use support::db::{self, Durable};
use support::erasure::{
    ITEMS_AT, MAIL_ERASED, MAIL_ITEMS_AT, MAIL_WITH_CONTENT, RESULTS_OF_JOB,
    assert_request_data_held,
};
use support::erasure_scene::child_body;
use support::fixture::{JobsFixture, runner_access};
use support::gesture::{self, Mail};
use support::lineage::{create, live_runner, started_with_agent};
use support::producer::{JobDeclaration, Producer};
use support::restart::{save_state, upload_result};
use support::tree::correspondents;
use support::{gql, runner};
use uuid::Uuid;

#[tokio::test]
async fn a_failed_run_erases_its_working_state_and_a_mail_whose_two_ends_are_removed() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let (mut x, mut y) = (
        live_runner(&fixture, "failure-x").await,
        live_runner(&fixture, "failure-y").await,
    );
    let declaration = JobDeclaration::new(&x.runner_type)
        .with_config(json!({ "model": "m" }))
        .with_max_attempts(2);
    let mut body = declaration.payload("projects");
    body["payload"] = json!({ "form": "INLINE", "message": "the brief", "attachments": [] });
    Producer::new(fixture.fabric(), "projects")
        .declare_payload(body)
        .await;
    let (first, r) = started_with_agent(&fixture, &mut x).await;
    let run5 = support::gesture::Work::of(&x, &first);
    save_state(&fixture, &run5, json!({ "turn": 1 })).await;
    let called = create(
        &fixture,
        &run5,
        &y.runner_type,
        declaration.job_id,
        child_body("SYNC"),
    )
    .await;
    let (d_trigger, t) = started_with_agent(&fixture, &mut y).await;
    assert_eq!(runner::job_id(&d_trigger), called);
    let run6 = support::gesture::Work::of(&y, &d_trigger);
    let mail = Mail::new(t, &[r]);
    gesture::accepted(
        &gesture::send_mail(&fixture.gql(), &runner_access(), &run6, &mail).await,
        "jobsSendMail",
    );
    upload_result(&fixture, &run6, "the answer of D").await;
    durable
        .assert_count(MAIL_ITEMS_AT, r, 1, "a mail waits unread at R")
        .await;

    y.fail_run(&d_trigger, "PERMANENT", "provider_refused", None)
        .await;
    durable
        .await_count(db::AGENTS_OF_RUN, run6.run_id, 0, "T is removed with Run 6")
        .await;
    durable
        .assert_count(
            MAIL_WITH_CONTENT,
            mail.mail_id,
            1,
            "the mail keeps its content, R is live",
        )
        .await;
    durable
        .assert_count(
            RESULTS_OF_JOB,
            called,
            1,
            "the Result addressed to Run 5 stays",
        )
        .await;

    x.fail_run(&first, "TRANSIENT", "provider_unavailable", Some(0))
        .await;
    durable
        .await_count(db::AGENTS_OF_RUN, run5.run_id, 0, "R is removed with Run 5")
        .await;

    durable
        .assert_count(
            db::SAVED_STATES_OF_RUN,
            run5.run_id,
            0,
            "the saved state is erased",
        )
        .await;
    durable
        .assert_count(ITEMS_AT, r, 0, "no item waits at R")
        .await;
    durable
        .assert_count(
            RESULTS_OF_JOB,
            called,
            0,
            "the Result addressed to Run 5 is erased",
        )
        .await;
    durable
        .assert_count(
            MAIL_ERASED,
            mail.mail_id,
            1,
            "the mail loses its content, both ends are removed",
        )
        .await;
    assert_request_data_held(
        &fixture,
        &durable,
        declaration.job_id,
        "a failed Run is not a cancel",
    )
    .await;

    let (retry, r2) = started_with_agent(&fixture, &mut x).await;
    let refused = gesture::send_mail(
        &fixture.gql(),
        &runner_access(),
        &support::gesture::Work::of(&x, &retry),
        &Mail::new(r2, &[r]),
    )
    .await;
    assert_eq!(gesture::refusal(&refused, "jobsSendMail"), "bad_addresses");
    assert_eq!(
        gql::status_of(&fixture.gql(), fixture.admin(), declaration.job_id).await,
        "IN_PROGRESS"
    );
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn an_attachment_for_a_mail_whose_content_was_erased_is_refused_with_a_reason() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let tree = correspondents(&fixture, "erased-mail").await;
    let mail = Mail::new(tree.sender, &[tree.receiver]);
    gesture::accepted(
        &gesture::send_mail(&fixture.gql(), &runner_access(), &tree.parent_work, &mail).await,
        "jobsSendMail",
    );
    sqlx::query("UPDATE mails SET content_erased_at = now() WHERE mail_id = $1")
        .bind(mail.mail_id)
        .execute(durable.pool())
        .await
        .expect("the mail is marked erased");

    let refused = gesture::upload_attachment(
        &fixture.gql(),
        &runner_access(),
        &tree.parent_work,
        &mail,
        Uuid::now_v7(),
        "late.txt",
        "late",
    )
    .await;
    assert_eq!(
        gesture::refusal(&refused, "jobsUploadAttachment"),
        "mail_content_erased"
    );
    durable.close().await;
    fixture.shutdown().await;
}
