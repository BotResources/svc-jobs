mod support;

use serde_json::json;
use support::db::Durable;
use support::fixture::{JobsFixture, runner_access};
use support::gesture::{self};
use support::restart::{RESTARTS_OF_RUN, running_child};
use support::{LONG, QUIET, gql};

#[tokio::test]
async fn an_agent_under_a_terminal_parent_refuses_the_mail_and_is_never_restarted() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let mut scene = running_child(&fixture, "restart-terminal-parent").await;
    scene.finish_child(&durable).await;
    support::producer::Producer::new(fixture.fabric(), "projects")
        .finish_v2(scene.root)
        .await;
    gql::wait_for_status(
        &fixture.gql(),
        fixture.admin(),
        scene.root,
        "COMPLETED",
        LONG,
    )
    .await;

    let mail = gesture::Mail::new(scene.sender, &[scene.receiver]);
    let refused =
        gesture::send_mail(&fixture.gql(), &runner_access(), &scene.parent_work, &mail).await;
    assert_eq!(
        gesture::refusal(&refused, "jobsSendMail"),
        "bad_addresses",
        "{refused}"
    );
    scene.pair.child.expect_no_trigger(QUIET).await;
    durable
        .assert_count(RESTARTS_OF_RUN, scene.child_run(), 0, "no restart")
        .await;
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn two_mails_at_once_to_a_finished_agent_start_exactly_one_restart() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let mut scene = running_child(&fixture, "restart-race").await;
    scene.finish_child(&durable).await;

    let gql = fixture.gql();
    let passport = runner_access();
    let first = gesture::Mail::new(scene.sender, &[scene.receiver]);
    let second = gesture::Mail::new(scene.peer, &[scene.receiver]);
    let (one, two) = tokio::join!(
        gesture::send_mail(&gql, &passport, &scene.parent_work, &first),
        gesture::send_mail(&gql, &passport, &scene.parent_work, &second),
    );
    gesture::accepted(&one, "jobsSendMail");
    gesture::accepted(&two, "jobsSendMail");

    let trigger = scene.pair.child.next_trigger(LONG).await;
    assert!(trigger["restart"].is_object());
    scene.pair.child.expect_no_trigger(QUIET).await;
    durable
        .assert_count(
            RESTARTS_OF_RUN,
            scene.child_run(),
            1,
            "exactly one restart Job",
        )
        .await;
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn unread_mail_at_the_end_of_a_successful_run_starts_the_restart_at_once() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let mut scene = running_child(&fixture, "restart-at-end").await;

    let mail = gesture::Mail::new(scene.sender, &[scene.receiver]);
    let sent =
        gesture::send_mail(&fixture.gql(), &runner_access(), &scene.parent_work, &mail).await;
    gesture::accepted(&sent, "jobsSendMail");
    scene.pair.child.expect_no_trigger(QUIET).await;
    durable
        .assert_count(
            RESTARTS_OF_RUN,
            scene.child_run(),
            0,
            "the agent still runs",
        )
        .await;

    scene.finish_child(&durable).await;
    let trigger = scene.pair.child.next_trigger(LONG).await;
    assert_eq!(
        trigger["restart"]["restart_of_run_id"],
        json!(scene.child_run().to_string())
    );
    durable.close().await;
    fixture.shutdown().await;
}
