mod support;

use serde_json::Value;
use support::call_notices::{notices, wait_for_job_notice};
use support::calls::call_scene;
use support::fixture::JobsFixture;
use support::gesture::Work;
use support::producer::Producer;
use support::restart::upload_result;
use support::runner;
use support::{LONG, gql, wire};
use uuid::Uuid;

fn of_job(items: &[Value], job: Uuid, notice_type: &str) -> Vec<Value> {
    items
        .iter()
        .filter(|item| item["jobId"] == job.to_string() && item["noticeType"] == notice_type)
        .cloned()
        .collect()
}

#[tokio::test]
async fn a_background_call_tells_its_queue_position_each_time_it_starts_to_wait_and_dies_with_its_caller()
 {
    let fixture = JobsFixture::start().await;
    let gql = fixture.gql();
    let mut scene = call_scene(&fixture, "bg-life", 1).await;
    let busy = scene.call(&fixture, "BACKGROUND", 1).await;
    let busy_trigger = scene.called.next_trigger(LONG).await;
    assert_eq!(runner::job_id(&busy_trigger), busy);
    let retried = scene.call(&fixture, "BACKGROUND", 2).await;
    let behind = scene.call(&fixture, "BACKGROUND", 1).await;
    let seen = wait_for_job_notice(
        &fixture,
        &scene.work,
        Some(scene.address),
        "QUEUE_POSITION",
        Some(behind),
    )
    .await;
    assert_eq!(
        of_job(&seen, retried, "ACCEPTED").len(),
        1,
        "the Job was accepted at once"
    );
    assert_eq!(of_job(&seen, retried, "QUEUE_POSITION")[0]["position"], 1);
    assert_eq!(
        of_job(&seen, behind, "QUEUE_POSITION")[0]["position"],
        2,
        "the position among the waiting Jobs of Y in dispatch order"
    );

    scene.called.start_applied(&fixture, &busy_trigger).await;
    scene.called.complete_run(&busy_trigger).await;
    let done = wait_for_job_notice(
        &fixture,
        &scene.work,
        Some(scene.address),
        "RUN_COMPLETED",
        Some(busy),
    )
    .await;
    assert_eq!(of_job(&done, busy, "RUN_COMPLETED").len(), 1);
    assert_eq!(
        gql::status_of(&gql, fixture.admin(), busy).await,
        "IN_PROGRESS",
        "the Job is not finished by the end of its Run"
    );
    let moved = scene.called.next_trigger(LONG).await;
    assert_eq!(runner::job_id(&moved), retried);
    let after_change = notices(&fixture, &scene.work, Some(scene.address)).await;
    assert_eq!(
        of_job(&after_change, behind, "QUEUE_POSITION").len(),
        1,
        "a change of place is not told; only a new wait is"
    );

    scene.called.start_applied(&fixture, &moved).await;
    scene
        .called
        .fail_run(&moved, "TRANSIENT", "provider_unavailable", Some(0))
        .await;
    let waited_again = br_test_harness::wait_until(LONG, || async {
        of_job(
            &notices(&fixture, &scene.work, Some(scene.address)).await,
            retried,
            "QUEUE_POSITION",
        )
        .len()
            == 2
    })
    .await;
    assert!(
        waited_again,
        "a Job that starts to wait again is told its place again"
    );

    Producer::new(fixture.fabric(), "projects")
        .cancel_v2(scene.root)
        .await;
    for called in [busy, retried, behind] {
        gql::wait_for_status(&gql, fixture.admin(), called, "CANCELLED", LONG).await;
    }
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_sync_call_stays_in_progress_until_its_caller_finishes_it_and_dies_with_its_caller() {
    let fixture = JobsFixture::start().await;
    let gql = fixture.gql();
    let mut scene = call_scene(&fixture, "sync-life", 3).await;

    let answered = scene.call(&fixture, "SYNC", 1).await;
    let trigger = scene.called.next_trigger(LONG).await;
    scene.called.start_applied(&fixture, &trigger).await;
    upload_result(&fixture, &Work::of(&scene.called, &trigger), "the answer").await;
    scene.called.complete_run(&trigger).await;
    let done =
        wait_for_job_notice(&fixture, &scene.work, None, "RUN_COMPLETED", Some(answered)).await;
    assert_eq!(
        of_job(&done, answered, "RUN_COMPLETED")[0]["answer"]["message"],
        "the answer"
    );
    assert_eq!(
        gql::status_of(&gql, fixture.admin(), answered).await,
        "IN_PROGRESS"
    );
    scene.resolve(&fixture, wire::VERB_FINISH, answered).await;
    gql::wait_for_status(&gql, fixture.admin(), answered, "COMPLETED", LONG).await;

    let doomed = scene.call(&fixture, "SYNC", 1).await;
    let doomed_trigger = scene.called.next_trigger(LONG).await;
    scene.called.start_applied(&fixture, &doomed_trigger).await;
    scene
        .called
        .fail_run(&doomed_trigger, "PERMANENT", "provider_refused", None)
        .await;
    let failed = wait_for_job_notice(&fixture, &scene.work, None, "JOB_FAILED", Some(doomed)).await;
    let failure = &of_job(&failed, doomed, "JOB_FAILED")[0];
    assert_eq!(failure["failureReport"]["reason_code"], "provider_refused");
    let message = failure["message"].as_str().unwrap_or_default();
    assert!(
        message.contains(&doomed.to_string())
            && message.contains("provider_refused")
            && message.contains("permanent"),
        "the failure comes with a message in plain words: {message}"
    );

    let in_flight = scene.call(&fixture, "SYNC", 1).await;
    let flight = scene.called.next_trigger(LONG).await;
    scene.called.start_applied(&fixture, &flight).await;
    Producer::new(fixture.fabric(), "projects")
        .cancel_v2(scene.root)
        .await;
    gql::wait_for_status(&gql, fixture.admin(), in_flight, "CANCELLED", LONG).await;
    scene
        .called
        .await_cancel_entry(runner::run_id(&flight), LONG)
        .await;
    fixture.shutdown().await;
}
