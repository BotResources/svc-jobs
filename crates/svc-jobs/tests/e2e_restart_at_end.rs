mod support;

use support::db::Durable;
use support::fixture::{JobsFixture, runner_access};
use support::gesture::{self, Mail, Work};
use support::restart::{RESTARTS_OF_RUN, UNREAD_AT, running_child};
use support::{LONG, QUIET};

#[tokio::test]
async fn the_mail_left_unread_at_the_end_of_a_run_is_delivered_unread_to_the_restart() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let mut scene = running_child(&fixture, "end-unread").await;
    let mail = Mail::new(scene.sender, &[scene.receiver]).with_body("read me later");
    gesture::accepted(
        &gesture::send_mail(&fixture.gql(), &runner_access(), &scene.parent_work, &mail).await,
        "jobsSendMail",
    );
    scene.finish_child(&durable).await;
    let restart = scene.pair.child.next_trigger(LONG).await;
    scene.pair.child.start_applied(&fixture, &restart).await;
    let waiting = gesture::items(
        &gesture::unread(
            &fixture.gql(),
            &runner_access(),
            &Work::of(&scene.pair.child, &restart),
            Some(scene.receiver),
        )
        .await,
    );
    assert_eq!(waiting.len(), 1, "the mail is delivered to the restart Run");
    assert_eq!(waiting[0]["mailId"], mail.mail_id.to_string());
    assert_eq!(waiting[0]["body"], "read me later", "unread, whole");
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_cc_notice_left_unread_at_the_end_of_a_run_starts_nothing() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let mut scene = running_child(&fixture, "end-cc").await;
    let copied = Mail::new(scene.sender, &[scene.peer]).cc(&[scene.receiver]);
    gesture::accepted(
        &gesture::send_mail(
            &fixture.gql(),
            &runner_access(),
            &scene.parent_work,
            &copied,
        )
        .await,
        "jobsSendMail",
    );
    scene.finish_child(&durable).await;
    scene.pair.child.expect_no_trigger(QUIET).await;
    durable
        .assert_count(
            RESTARTS_OF_RUN,
            scene.child_run(),
            0,
            "an unread cc notice starts nothing",
        )
        .await;
    durable
        .assert_count(UNREAD_AT, scene.receiver, 1, "the notice stays unread")
        .await;
    durable.close().await;
    fixture.shutdown().await;
}
