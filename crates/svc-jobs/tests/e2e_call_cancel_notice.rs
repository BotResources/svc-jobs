mod support;

use serde_json::Value;
use support::call_notices::{notices, wait_for_job_notice};
use support::calls::call_scene;
use support::fixture::JobsFixture;
use support::gesture::{self, Work};
use support::lineage::act;
use support::{LONG, gql, wire};
use uuid::Uuid;

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
    let client = fixture.gql();
    let mut scene = call_scene(&fixture, "sync-cancel", 2).await;

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

    let item = Uuid::parse_str(told[0]["itemId"].as_str().unwrap_or_default()).unwrap();
    gesture::mark_read(fixture.nats(), &scene.caller, scene.work.run_id, &[item]).await;
    let after_marker = notices(&fixture, &scene.work, None).await;
    assert!(
        cancelled_notices(&after_marker, by_administrator).is_empty()
            && cancelled_notices(&after_marker, by_its_runner).len() == 1,
        "the read marker covers the notice it names and no other: {after_marker:?}"
    );
    fixture.shutdown().await;
}
