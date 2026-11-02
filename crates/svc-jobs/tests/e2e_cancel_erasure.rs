mod support;

use serde_json::json;
use support::call_notices::wait_for_job_notice;
use support::db::{self, Durable};
use support::erasure::{
    ATTACHMENTS_OF_MAIL, ITEMS_AT, ITEMS_FOR_RUN, MAIL_ERASED, MAIL_ITEMS_AT, MAIL_WITH_CONTENT,
    RECIPIENT_ROWS_OF_MAIL, RESULTS_OF_JOB, assert_request_data_erased, assert_request_data_held,
};
use support::erasure_scene::{Scene, build};
use support::fixture::{JobsFixture, runner_access};
use support::gesture::{self, Mail};
use support::restart::read_result;
use support::{LONG, gql};
use uuid::Uuid;

async fn cancel(fixture: &JobsFixture, job: Uuid) {
    gql::cancel_job(&fixture.gql(), fixture.admin(), Uuid::now_v7(), job).await;
    gql::wait_for_status(&fixture.gql(), fixture.admin(), job, "CANCELLED", LONG).await;
}

async fn assert_working_state_of_b_erased(fixture: &JobsFixture, durable: &Durable, s: &Scene<'_>) {
    durable
        .assert_count(db::RUNS_OF_JOB, s.b, 1, "B keeps its Run")
        .await;
    durable
        .assert_count(
            db::AGENTS_OF_RUN,
            s.b_work.run_id,
            0,
            "Q and its address are removed",
        )
        .await;
    durable
        .assert_count(
            db::SAVED_STATES_OF_RUN,
            s.b_work.run_id,
            0,
            "the saved state of Q is erased",
        )
        .await;
    durable
        .assert_count(ITEMS_AT, s.q, 0, "no item waits at the address of Q")
        .await;
    durable
        .assert_count(MAIL_ITEMS_AT, s.q, 0, "in particular no MAIL item")
        .await;
    let to_q = Mail::new(s.p, &[s.q]);
    let refused = gesture::send_mail(&fixture.gql(), &runner_access(), &s.a_work, &to_q).await;
    assert_eq!(gesture::refusal(&refused, "jobsSendMail"), "bad_addresses");
    assert_eq!(
        gesture::field(&refused, "jobsSendMail")["badAddresses"][0]["reason"],
        "recipient_removed"
    );
    durable
        .assert_count(RESULTS_OF_JOB, s.b, 0, "the Result of B is erased")
        .await;
    let read = read_result(fixture, &s.a_work, s.b_result).await;
    assert_eq!(gesture::refusal(&read, "jobsReadResult"), "result_unknown");
}

#[tokio::test]
async fn a_cancel_erases_the_working_state_of_the_cancelled_job_and_nothing_a_live_agent_needs() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let s = build(&fixture, &durable, "erase-cancel").await;
    for (job, what) in [(s.a, "A"), (s.b, "B"), (s.c, "C")] {
        assert_request_data_held(&fixture, &durable, job, what).await;
    }
    durable
        .assert_count(MAIL_ITEMS_AT, s.q, 1, "a mail waits unread at Q")
        .await;
    durable
        .assert_count(RESULTS_OF_JOB, s.e, 1, "E left a Result for Run 1")
        .await;

    let before = gql::job(&fixture.gql(), fixture.admin(), s.b).await;

    cancel(&fixture, s.b).await;

    assert_working_state_of_b_erased(&fixture, &durable, &s).await;
    let after = gql::job(&fixture.gql(), fixture.admin(), s.b).await;
    assert_eq!(after["status"], "CANCELLED", "{after}");
    assert_eq!(after["attemptCount"], before["attemptCount"], "{after}");
    assert_eq!(after["parentJobId"], json!(s.a.to_string()), "{after}");
    assert_eq!(after["createdAt"], before["createdAt"], "{after}");
    assert_eq!(after["resolution"]["kind"], "CANCELLED", "{after}");
    assert!(after["resolution"]["occurredAt"].is_string(), "{after}");
    assert_eq!(after["runs"][0]["id"], before["runs"][0]["id"], "{after}");
    assert_eq!(
        after["runs"][0]["startedAt"], before["runs"][0]["startedAt"],
        "{after}"
    );
    assert!(after["runs"][0]["finishedAt"].is_string(), "{after}");
    durable
        .assert_count(RESULTS_OF_JOB, s.e, 1, "the Result of E stays")
        .await;
    wait_for_job_notice(&fixture, &s.a_work, None, "JOB_CANCELLED", Some(s.b)).await;
    for (mail, what) in [
        (s.mail_p_s, "between P and S"),
        (s.mail_to_q, "from P to the removed Q"),
    ] {
        durable
            .assert_count(
                MAIL_WITH_CONTENT,
                mail,
                1,
                &format!("the mail {what} keeps its content, one end is live"),
            )
            .await;
    }
    durable
        .assert_count(ATTACHMENTS_OF_MAIL, s.mail_p_s, 1, "with its attachment")
        .await;
    assert_request_data_erased(&fixture, &durable, s.b, "B").await;
    assert_request_data_held(&fixture, &durable, s.a, "A").await;
    assert_request_data_held(&fixture, &durable, s.c, "C").await;

    cancel(&fixture, s.a).await;
    gql::wait_for_status(&fixture.gql(), fixture.admin(), s.c, "CANCELLED", LONG).await;

    for (run, what) in [(s.a_work.run_id, "P"), (s.c_work.run_id, "S")] {
        durable
            .assert_count(db::AGENTS_OF_RUN, run, 0, &format!("{what} is removed"))
            .await;
    }
    for (mail, what) in [
        (s.mail_p_s, "between P and S"),
        (s.mail_to_q, "from P to Q"),
    ] {
        durable
            .assert_count(
                MAIL_ERASED,
                mail,
                1,
                &format!("the mail {what} loses its content"),
            )
            .await;
        durable
            .assert_count(
                RECIPIENT_ROWS_OF_MAIL,
                mail,
                1,
                &format!("the mail {what} keeps its recipients"),
            )
            .await;
    }
    durable
        .assert_count(ATTACHMENTS_OF_MAIL, s.mail_p_s, 0, "and its attachments")
        .await;
    durable
        .assert_count(RESULTS_OF_JOB, s.e, 0, "the Result of E goes with Run 1")
        .await;
    durable
        .assert_count(
            ITEMS_FOR_RUN,
            s.a_work.run_id,
            0,
            "the unread items of Run 1 go",
        )
        .await;
    assert_request_data_erased(&fixture, &durable, s.a, "A").await;
    assert_request_data_erased(&fixture, &durable, s.c, "C").await;
    durable.close().await;
    fixture.shutdown().await;
}
