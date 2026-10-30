mod support;

use serde_json::Value;
use support::db::Durable;
use support::fixture::JobsFixture;
use support::restart::{RESTARTS_OF_RUN, Scene, UNREAD_AT, running_child};
use support::surface;
use support::{LONG, QUIET};
use uuid::Uuid;

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
    let _trigger = scene.pair.child.next_trigger(LONG).await;
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
    durable.close().await;
    fixture.shutdown().await;
}
