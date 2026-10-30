mod support;

use serde_json::json;
use support::db::Durable;
use support::events::DoorbellLog;
use support::fixture::{JobsFixture, runner_access};
use support::gesture::{self, Mail};
use support::producer::Producer;
use support::restart::running_child;
use support::{LONG, wire};
use uuid::Uuid;

#[tokio::test]
async fn every_kind_of_item_rings_the_live_run_and_a_runner_off_the_bus_loses_nothing() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let gql = fixture.gql();
    let runner = runner_access();
    let scene = running_child(&fixture, "doorbell").await;
    let run = scene.child_run();

    let missed = [
        Mail::new(scene.sender, &[scene.receiver]).with_body("while away 1"),
        Mail::new(scene.peer, &[scene.receiver]).with_body("while away 2"),
    ];
    for mail in &missed {
        gesture::accepted(
            &gesture::send_mail(&gql, &runner, &scene.parent_work, mail).await,
            "jobsSendMail",
        );
    }
    let bell = DoorbellLog::open(fixture.fabric()).await;
    let waiting = gesture::items(
        &gesture::unread(&gql, &runner, &scene.child_work, Some(scene.receiver)).await,
    );
    let bodies: Vec<&str> = waiting
        .iter()
        .filter_map(|item| item["body"].as_str())
        .collect();
    assert_eq!(
        bodies,
        vec!["while away 1", "while away 2"],
        "both mails, in order, after the reconnection"
    );
    let items: Vec<Uuid> = waiting
        .iter()
        .filter_map(|item| item["itemId"].as_str().and_then(|raw| raw.parse().ok()))
        .collect();
    let marked = gesture::mark_read(fixture.nats(), &scene.pair.child, run, &items).await;
    assert_eq!(marked["accepted"], true, "{marked}");
    assert!(
        gesture::items(
            &gesture::unread(&gql, &runner, &scene.child_work, Some(scene.receiver)).await
        )
        .is_empty(),
        "none twice after their markers"
    );

    let copied = Mail::new(scene.sender, &[scene.peer]).cc(&[scene.receiver]);
    gesture::accepted(
        &gesture::send_mail(&gql, &runner, &scene.parent_work, &copied).await,
        "jobsSendMail",
    );
    let rang = bell.expect_rings(run, 3, LONG).await;
    let cc = rang
        .iter()
        .find(|ring| ring["item_kind"] == "CC_NOTICE")
        .unwrap_or_else(|| panic!("a cc notice is announced at once: {rang:?}"));
    assert_eq!(cc["address"], scene.receiver.to_string());

    let called = Uuid::now_v7();
    Producer::new(fixture.fabric(), "projects")
        .send_v3(
            wire::VERB_CREATE,
            json!({
                "job_id": called.to_string(),
                "runner_type": scene.pair.parent.runner_type,
                "producer": "projects",
                "parent_job_id": scene.child_work.job_id.to_string(),
                "mode": "BACKGROUND",
                "caller_address": scene.receiver.to_string(),
                "acting_run_id": run.to_string(),
                "acting_instance_key": scene.pair.child.instance_key,
            }),
        )
        .await;
    let rings = bell.expect_rings(run, 4, LONG).await;
    assert!(
        rings.iter().any(|ring| ring["item_kind"] == "NOTICE"),
        "a notice of a runner call is announced the same way: {rings:?}"
    );
    assert_eq!(durable.rings_for(run, Some("CC_NOTICE")).await, 1);
    bell.stop().await;
    durable.close().await;
    fixture.shutdown().await;
}
