mod support;

use serde_json::Value;
use support::db::Durable;
use support::fixture::{JobsFixture, runner_access};
use support::gesture;
use support::restart::{RESTARTS_OF_RUN, Scene, UNREAD_AT, running_child};
use support::runner;
use support::surface;
use support::{LONG, QUIET};
use uuid::Uuid;

async fn rings_for(durable: &Durable, run: Uuid) -> i64 {
    durable.rings_for(run, None).await
}

const WAKES_OF_RUN: &str = "SELECT count(*) FROM restart_wakes WHERE run_id = $1";

async fn restartable_now(fixture: &JobsFixture, scene: &Scene<'_>, agent: Uuid) -> Value {
    let listing = surface::list(fixture.nats(), &scene.pair.parent, scene.parent_run()).await;
    listing["agents"]
        .as_array()
        .and_then(|agents| {
            agents
                .iter()
                .find(|listed| listed["agent_id"] == agent.to_string())
        })
        .map(|listed| listed["restartable_now"].clone())
        .unwrap_or_else(|| panic!("the agent is listed: {listing}"))
}

#[tokio::test]
async fn while_a_restart_job_is_live_the_listing_the_mail_and_the_wake_agree_there_is_nothing_more_to_start()
 {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let mut scene = running_child(&fixture, "restart-in-flight").await;
    scene.finish_child(&durable).await;
    assert_eq!(
        restartable_now(&fixture, &scene, scene.receiver).await,
        true,
        "before any mail the finished agent is restartable"
    );

    scene.mail_receiver(&fixture, scene.sender).await;
    let trigger = scene.pair.child.next_trigger(LONG).await;
    durable
        .assert_count(RESTARTS_OF_RUN, scene.child_run(), 1, "one restart Job")
        .await;
    assert_eq!(
        restartable_now(&fixture, &scene, scene.receiver).await,
        false,
        "the restart Job is live: the listing no longer offers a restart"
    );

    scene.mail_receiver(&fixture, scene.peer).await;
    scene.pair.child.expect_no_trigger(QUIET).await;
    durable
        .assert_count(
            RESTARTS_OF_RUN,
            scene.child_run(),
            1,
            "the second mail starts no second restart",
        )
        .await;
    durable
        .assert_count(
            WAKES_OF_RUN,
            scene.child_run(),
            0,
            "no wake waits for a restart already live",
        )
        .await;
    durable
        .assert_count(
            UNREAD_AT,
            scene.receiver,
            2,
            "both mails wait unread for the restart Run",
        )
        .await;
    assert_eq!(
        rings_for(&durable, runner::run_id(&trigger)).await,
        0,
        "a restart Run that has not started is rung by nothing"
    );
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_mail_reaching_an_agent_whose_restart_run_is_started_rings_that_run_once() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let mut scene = running_child(&fixture, "restart-ring-mail").await;
    scene.finish_child(&durable).await;
    scene.mail_receiver(&fixture, scene.sender).await;
    let trigger = scene.pair.child.next_trigger(LONG).await;
    scene.pair.child.start_applied(&fixture, &trigger).await;
    let restart_run = runner::run_id(&trigger);
    assert_eq!(rings_for(&durable, restart_run).await, 0);

    scene.mail_receiver(&fixture, scene.peer).await;

    assert_eq!(
        rings_for(&durable, restart_run).await,
        1,
        "the second mail rings the live restart Run, before its agents are registered"
    );
    durable
        .assert_count(RESTARTS_OF_RUN, scene.child_run(), 1, "no second restart")
        .await;
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_cc_notice_reaching_an_agent_whose_restart_run_is_started_rings_that_run_once() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let mut scene = running_child(&fixture, "restart-ring-cc").await;
    scene.finish_child(&durable).await;
    scene.mail_receiver(&fixture, scene.sender).await;
    let trigger = scene.pair.child.next_trigger(LONG).await;
    scene.pair.child.start_applied(&fixture, &trigger).await;
    let restart_run = runner::run_id(&trigger);

    let notice = gesture::Mail::new(scene.sender, &[scene.peer]).cc(&[scene.receiver]);
    let sent = gesture::send_mail(
        &fixture.gql(),
        &runner_access(),
        &scene.parent_work,
        &notice,
    )
    .await;
    gesture::accepted(&sent, "jobsSendMail");

    assert_eq!(
        rings_for(&durable, restart_run).await,
        1,
        "the cc notice rings the live restart Run"
    );
    durable
        .assert_count(RESTARTS_OF_RUN, scene.child_run(), 1, "no second restart")
        .await;
    durable.close().await;
    fixture.shutdown().await;
}
