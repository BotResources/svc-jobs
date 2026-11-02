mod support;

use serde_json::Value;
use support::call_notices::{notices, wait_for_job_notice};
use support::calls::call_scene;
use support::db::{self, Durable};
use support::fixture::JobsFixture;
use support::gesture::{self, Work};
use support::lineage::act;
use support::{LONG, gql, wire};
use uuid::Uuid;

const ITEMS_OF_RUN: &str = "SELECT count(*) FROM inbox_items WHERE run_id = $1";

fn cancelled_notices(items: &[Value], job: Uuid) -> Vec<Value> {
    items
        .iter()
        .filter(|item| item["jobId"] == job.to_string() && item["noticeType"] == "JOB_CANCELLED")
        .cloned()
        .collect()
}

#[tokio::test]
async fn a_sync_call_whose_called_job_is_cancelled_tells_the_caller_whoever_cancelled_it() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let client = fixture.gql();
    let mut scene = call_scene(&fixture, "sync-cancel", 3).await;

    let by_administrator = scene.call(&fixture, "SYNC", 1).await;
    let first = scene.called.next_trigger(LONG).await;
    scene.called.start_applied(&fixture, &first).await;
    gql::cancel_job(&client, fixture.admin(), Uuid::now_v7(), by_administrator).await;
    gql::wait_for_status(
        &client,
        fixture.admin(),
        by_administrator,
        "CANCELLED",
        LONG,
    )
    .await;
    let seen = wait_for_job_notice(
        &fixture,
        &scene.work,
        None,
        "JOB_CANCELLED",
        Some(by_administrator),
    )
    .await;
    let told = cancelled_notices(&seen, by_administrator);
    assert_eq!(told.len(), 1, "one notice for one cancelled call: {seen:?}");
    assert_eq!(told[0]["itemKind"], "NOTICE");
    let item = Uuid::parse_str(told[0]["itemId"].as_str().unwrap_or_default()).unwrap();
    assert_eq!(
        durable.rings_of_item(item).await,
        1,
        "the notice rings the arrival doorbell of the live Caller Run once"
    );
    let words = told[0]["message"].as_str().unwrap_or_default();
    assert!(
        words.contains(&by_administrator.to_string()) && words.contains("cancelled"),
        "the notice has a message in plain words: {words}"
    );

    let by_its_runner = scene.call(&fixture, "SYNC", 1).await;
    let second = scene.called.next_trigger(LONG).await;
    scene.called.start_applied(&fixture, &second).await;
    let working = Work::of(&scene.called, &second);
    act(&fixture, wire::VERB_CANCEL, by_its_runner, &working).await;
    gql::wait_for_status(&client, fixture.admin(), by_its_runner, "CANCELLED", LONG).await;
    let seen = wait_for_job_notice(
        &fixture,
        &scene.work,
        None,
        "JOB_CANCELLED",
        Some(by_its_runner),
    )
    .await;
    assert_eq!(cancelled_notices(&seen, by_its_runner).len(), 1);

    let by_requester = scene.call(&fixture, "SYNC", 1).await;
    let third = scene.called.next_trigger(LONG).await;
    scene.called.start_applied(&fixture, &third).await;
    scene
        .resolve(&fixture, wire::VERB_CANCEL, by_requester)
        .await;
    gql::wait_for_status(&client, fixture.admin(), by_requester, "CANCELLED", LONG).await;
    let seen = wait_for_job_notice(
        &fixture,
        &scene.work,
        None,
        "JOB_CANCELLED",
        Some(by_requester),
    )
    .await;
    assert_eq!(cancelled_notices(&seen, by_requester).len(), 1);
    assert_eq!(
        cancelled_notices(&seen, by_administrator).len(),
        1,
        "the first notice stays unread and is not told again"
    );

    assert_eq!(
        gql::status_of(&client, fixture.admin(), scene.root).await,
        "IN_PROGRESS",
        "the cancel of a called Job does not end the Job of its Caller"
    );
    let at_address = notices(&fixture, &scene.work, Some(scene.address)).await;
    assert!(
        cancelled_notices(&at_address, by_administrator).is_empty(),
        "a sync call writes its notices for the Run, not for an address"
    );

    gesture::mark_read(fixture.nats(), &scene.caller, scene.work.run_id, &[item]).await;
    let after_marker = notices(&fixture, &scene.work, None).await;
    assert!(
        cancelled_notices(&after_marker, by_administrator).is_empty()
            && cancelled_notices(&after_marker, by_its_runner).len() == 1,
        "the read marker covers the notice it names and no other: {after_marker:?}"
    );
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_background_call_whose_called_job_is_cancelled_tells_the_caller_at_its_address_whoever_cancelled_it()
 {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let client = fixture.gql();
    let mut scene = call_scene(&fixture, "background-cancel", 3).await;

    let by_administrator = scene.call(&fixture, "BACKGROUND", 1).await;
    let first = scene.called.next_trigger(LONG).await;
    scene.called.start_applied(&fixture, &first).await;
    gql::cancel_job(&client, fixture.admin(), Uuid::now_v7(), by_administrator).await;
    gql::wait_for_status(
        &client,
        fixture.admin(),
        by_administrator,
        "CANCELLED",
        LONG,
    )
    .await;
    let seen = wait_for_job_notice(
        &fixture,
        &scene.work,
        Some(scene.address),
        "JOB_CANCELLED",
        Some(by_administrator),
    )
    .await;
    let told = cancelled_notices(&seen, by_administrator);
    assert_eq!(told.len(), 1, "one notice for one cancelled call: {seen:?}");
    assert_eq!(told[0]["itemKind"], "NOTICE");
    assert_eq!(told[0]["address"], scene.address.to_string());
    let item = Uuid::parse_str(told[0]["itemId"].as_str().unwrap_or_default()).unwrap();
    assert_eq!(durable.rings_of_item(item).await, 1);
    assert_eq!(durable.rings_of_item_v2(item).await, 1);
    let words = told[0]["message"].as_str().unwrap_or_default();
    assert!(
        words.contains(&by_administrator.to_string()) && words.contains("cancelled"),
        "the notice has a message in plain words: {words}"
    );

    let by_its_runner = scene.call(&fixture, "BACKGROUND", 1).await;
    let second = scene.called.next_trigger(LONG).await;
    scene.called.start_applied(&fixture, &second).await;
    act(
        &fixture,
        wire::VERB_CANCEL,
        by_its_runner,
        &Work::of(&scene.called, &second),
    )
    .await;
    gql::wait_for_status(&client, fixture.admin(), by_its_runner, "CANCELLED", LONG).await;
    let seen = wait_for_job_notice(
        &fixture,
        &scene.work,
        Some(scene.address),
        "JOB_CANCELLED",
        Some(by_its_runner),
    )
    .await;
    assert_eq!(cancelled_notices(&seen, by_its_runner).len(), 1);

    let by_requester = scene.call(&fixture, "BACKGROUND", 1).await;
    let third = scene.called.next_trigger(LONG).await;
    scene.called.start_applied(&fixture, &third).await;
    scene
        .resolve(&fixture, wire::VERB_CANCEL, by_requester)
        .await;
    gql::wait_for_status(&client, fixture.admin(), by_requester, "CANCELLED", LONG).await;
    let seen = wait_for_job_notice(
        &fixture,
        &scene.work,
        Some(scene.address),
        "JOB_CANCELLED",
        Some(by_requester),
    )
    .await;
    assert_eq!(cancelled_notices(&seen, by_requester).len(), 1);
    assert_eq!(
        cancelled_notices(&seen, by_administrator).len(),
        1,
        "the first notice stays unread and is not told again"
    );
    assert_eq!(
        gql::status_of(&client, fixture.admin(), scene.root).await,
        "IN_PROGRESS",
        "the cancel of a called Job does not end the Job of its Caller"
    );
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_caller_run_that_fails_keeps_no_cancel_notice_of_the_calls_the_failure_cancels() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let mut scene = call_scene(&fixture, "sync-failed", 2).await;

    let answered = scene.call(&fixture, "SYNC", 1).await;
    let trigger = scene.called.next_trigger(LONG).await;
    scene.called.start_applied(&fixture, &trigger).await;
    scene.called.complete_run(&trigger).await;
    durable
        .await_count(
            ITEMS_OF_RUN,
            scene.work.run_id,
            1,
            "the end of the call waits unread for the Caller Run",
        )
        .await;

    scene
        .caller
        .fail_run(&scene.trigger, "TRANSIENT", "provider_unavailable", Some(0))
        .await;
    gql::wait_for_status(&fixture.gql(), fixture.admin(), answered, "CANCELLED", LONG).await;
    durable
        .await_count(
            db::RESOLUTIONS_OF_JOB,
            answered,
            1,
            "the failure cancelled the open call",
        )
        .await;
    durable
        .assert_count(
            ITEMS_OF_RUN,
            scene.work.run_id,
            0,
            "the failed Caller Run keeps no notice",
        )
        .await;
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_caller_run_that_is_cancelled_keeps_no_cancel_notice_of_the_calls_the_cancel_ends() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let client = fixture.gql();
    let mut scene = call_scene(&fixture, "sync-caller-cancelled", 2).await;

    let answered = scene.call(&fixture, "SYNC", 1).await;
    let trigger = scene.called.next_trigger(LONG).await;
    scene.called.start_applied(&fixture, &trigger).await;

    gql::cancel_job(&client, fixture.admin(), Uuid::now_v7(), scene.root).await;
    gql::wait_for_status(&client, fixture.admin(), scene.root, "CANCELLED", LONG).await;
    gql::wait_for_status(&client, fixture.admin(), answered, "CANCELLED", LONG).await;
    durable
        .await_count(
            db::RESOLUTIONS_OF_JOB,
            answered,
            1,
            "the cancel of the Caller Job cancelled the open call",
        )
        .await;
    durable
        .assert_count(
            ITEMS_OF_RUN,
            scene.work.run_id,
            0,
            "the cancelled Caller Run keeps no notice",
        )
        .await;
    durable.close().await;
    fixture.shutdown().await;
}
