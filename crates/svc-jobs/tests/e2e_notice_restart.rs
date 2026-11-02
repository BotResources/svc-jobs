mod support;

use serde_json::json;
use support::call_notices::{
    background_call, caller, refused_background_call, wait_for_job_notice, wait_for_mail_notices,
    wait_for_notice,
};
use support::db::Durable;
use support::fixture::{JobsFixture, Knobs, runner_access};
use support::gesture::{self, Mail, Work};
use support::restart::{FINISHED_OF_RUN, save_state};
use support::{LONG, gql, runner};
use uuid::Uuid;

enum Unread {
    Failure,
    Cancel,
    Refusal,
    MailNotice,
}

async fn an_unread_notice_at_the_end_of_a_successful_run_starts_the_restart_at_once(
    label: &str,
    unread: Unread,
    item_kind: &str,
    notice_type: &str,
) {
    let fixture = JobsFixture::start().await;
    let mut caller = caller(&fixture, label).await;
    let parent_run = runner::run_id(&caller.trigger);
    save_state(&fixture, &caller.work, json!({ "turn": 1 })).await;

    match unread {
        Unread::Failure => {
            let call = background_call(&fixture, &mut caller).await;
            caller.pair.child.start_applied(&fixture, &call).await;
            caller
                .pair
                .child
                .fail_run(&call, "PERMANENT", "boom", None)
                .await;
            wait_for_notice(&fixture, &caller.work, Some(caller.address), "JOB_FAILED").await;
        }
        Unread::Cancel => {
            let call = background_call(&fixture, &mut caller).await;
            caller.pair.child.start_applied(&fixture, &call).await;
            gql::cancel_job(
                &fixture.gql(),
                fixture.admin(),
                Uuid::now_v7(),
                runner::job_id(&call),
            )
            .await;
            wait_for_notice(
                &fixture,
                &caller.work,
                Some(caller.address),
                "JOB_CANCELLED",
            )
            .await;
        }
        Unread::Refusal => {
            let job = refused_background_call(&fixture, &caller).await;
            wait_for_job_notice(
                &fixture,
                &caller.work,
                Some(caller.address),
                "REFUSED",
                Some(job),
            )
            .await;
        }
        Unread::MailNotice => {
            let bad = Mail::new(caller.address, &[Uuid::now_v7()]);
            gesture::send_mail(&fixture.gql(), &runner_access(), &caller.work, &bad).await;
            wait_for_mail_notices(&fixture, &caller.work, Some(caller.address), 1).await;
        }
    }

    caller.pair.parent.complete_run(&caller.trigger).await;
    let restart = caller.pair.parent.next_trigger(LONG).await;
    assert_eq!(
        restart["restart"]["restart_of_run_id"],
        json!(parent_run.to_string()),
        "the restart starts at once, with no new arrival"
    );
    caller.pair.parent.start_applied(&fixture, &restart).await;
    let restarted = Work::of(&caller.pair.parent, &restart);
    let waiting = gesture::items(
        &gesture::unread(
            &fixture.gql(),
            &runner_access(),
            &restarted,
            Some(caller.address),
        )
        .await,
    );
    assert!(
        waiting
            .iter()
            .any(|item| item["itemKind"] == item_kind && item["noticeType"] == notice_type),
        "the restart Run reads the {notice_type} notice unread: {waiting:?}"
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_failure_notice_left_unread_at_the_end_of_a_successful_run_starts_the_restart_at_once() {
    an_unread_notice_at_the_end_of_a_successful_run_starts_the_restart_at_once(
        "end-failure",
        Unread::Failure,
        "NOTICE",
        "JOB_FAILED",
    )
    .await;
}

#[tokio::test]
async fn a_cancel_notice_left_unread_at_the_end_of_a_successful_run_starts_the_restart_at_once() {
    an_unread_notice_at_the_end_of_a_successful_run_starts_the_restart_at_once(
        "end-cancel",
        Unread::Cancel,
        "NOTICE",
        "JOB_CANCELLED",
    )
    .await;
}

#[tokio::test]
async fn a_refusal_notice_left_unread_at_the_end_of_a_successful_run_starts_the_restart_at_once() {
    an_unread_notice_at_the_end_of_a_successful_run_starts_the_restart_at_once(
        "end-refusal",
        Unread::Refusal,
        "NOTICE",
        "REFUSED",
    )
    .await;
}

#[tokio::test]
async fn a_mail_notice_left_unread_at_the_end_of_a_successful_run_starts_the_restart_at_once() {
    an_unread_notice_at_the_end_of_a_successful_run_starts_the_restart_at_once(
        "end-mail-notice",
        Unread::MailNotice,
        "MAIL_NOTICE",
        "REFUSED",
    )
    .await;
}

#[tokio::test]
async fn a_cancel_notice_wakes_a_finished_background_caller() {
    let fixture = JobsFixture::start_with(Knobs {
        backstop_interval_seconds: 3_600,
        ..Knobs::default()
    })
    .await;
    let durable = Durable::open(&fixture.app_url()).await;
    let mut caller = caller(&fixture, "wake-cancel").await;
    let parent_run = runner::run_id(&caller.trigger);
    save_state(&fixture, &caller.work, json!({ "turn": 1 })).await;
    let call = background_call(&fixture, &mut caller).await;
    caller.pair.child.start_applied(&fixture, &call).await;
    caller.pair.parent.complete_run(&caller.trigger).await;
    durable
        .await_count(FINISHED_OF_RUN, parent_run, 1, "the caller finishes")
        .await;
    caller.pair.parent.expect_no_trigger(support::QUIET).await;

    gql::cancel_job(
        &fixture.gql(),
        fixture.admin(),
        Uuid::now_v7(),
        runner::job_id(&call),
    )
    .await;
    let restart = caller.pair.parent.next_trigger(LONG).await;
    assert_eq!(
        restart["restart"]["restart_of_run_id"],
        json!(parent_run.to_string())
    );
    caller.pair.parent.start_applied(&fixture, &restart).await;
    let restarted = Work::of(&caller.pair.parent, &restart);
    wait_for_job_notice(
        &fixture,
        &restarted,
        Some(caller.address),
        "JOB_CANCELLED",
        Some(runner::job_id(&call)),
    )
    .await;
    durable.close().await;
    fixture.shutdown().await;
}
