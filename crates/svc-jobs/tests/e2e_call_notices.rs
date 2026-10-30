mod support;

use serde_json::json;
use support::call_notices::{background_call, caller, types, wait_for_job_notice, wait_for_notice};
use support::db::Durable;
use support::fixture::{JobsFixture, runner_access};
use support::gesture::{self, Work};
use support::runner;
use support::tree::call_child;

#[tokio::test]
async fn a_background_call_tells_its_caller_every_step_at_the_address_it_named() {
    let fixture = JobsFixture::start().await;
    let mut caller = caller(&fixture, "notices-background").await;

    let first = background_call(&fixture, &mut caller).await;
    let seen = wait_for_notice(
        &fixture,
        &caller.work,
        Some(caller.address),
        "QUEUE_POSITION",
    )
    .await;
    assert_eq!(
        types(&seen),
        vec!["ACCEPTED", "QUEUE_POSITION"],
        "the call is confirmed at once, then its place in the wait is told"
    );
    assert_eq!(seen[1]["position"], json!(1));
    assert_eq!(seen[0]["jobId"], json!(runner::job_id(&first).to_string()));

    caller.pair.child.start_applied(&fixture, &first).await;
    wait_for_notice(&fixture, &caller.work, Some(caller.address), "STARTED").await;
    caller.pair.child.complete_run(&first).await;
    let done = wait_for_notice(
        &fixture,
        &caller.work,
        Some(caller.address),
        "RUN_COMPLETED",
    )
    .await;
    assert_eq!(
        types(&done),
        vec!["ACCEPTED", "QUEUE_POSITION", "STARTED", "RUN_COMPLETED"]
    );
    assert!(
        done[3]["answer"].is_null() && done[3]["resultId"].is_null(),
        "a background call keeps no Result: the answer comes by mail"
    );

    let second = background_call(&fixture, &mut caller).await;
    caller.pair.child.start_applied(&fixture, &second).await;
    caller
        .pair
        .child
        .fail_run(&second, "PERMANENT", "provider_refused", None)
        .await;
    let failed = wait_for_notice(&fixture, &caller.work, Some(caller.address), "JOB_FAILED").await;
    let report = failed
        .iter()
        .find(|item| item["noticeType"] == "JOB_FAILED")
        .expect("the failure notice");
    assert_eq!(report["failureReport"]["reason_code"], "provider_refused");
    assert!(
        report["message"]
            .as_str()
            .is_some_and(|message| message.contains("provider_refused")),
        "the failure comes with a message in plain words: {report}"
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_sync_call_tells_the_callers_run_the_end_with_the_answer_or_a_reference_and_the_failure()
{
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let mut caller = caller(&fixture, "notices-sync").await;
    let parent_run = runner::run_id(&caller.trigger);

    let small = call_child(&fixture, &mut caller.pair, caller.root, parent_run).await;
    caller.pair.child.start_applied(&fixture, &small).await;
    let small_work = Work::of(&caller.pair.child, &small);
    let uploaded = fixture
        .gql()
        .query(
            &runner_access(),
            gesture::UPLOAD_RESULT,
            json!({ "input": { "work": small_work.wire(), "answer": gesture::content("42", &[]) } }),
        )
        .await;
    gesture::accepted(&uploaded, "jobsUploadResult");
    caller.pair.child.complete_run(&small).await;
    let seen = wait_for_notice(&fixture, &caller.work, None, "RUN_COMPLETED").await;
    assert_eq!(
        types(&seen),
        vec!["RUN_COMPLETED"],
        "a sync call tells only the end"
    );
    assert_eq!(seen[0]["answer"]["message"], "42");
    assert!(
        seen[0]["resultId"].is_string(),
        "the Result stays readable by reference"
    );

    let large = call_child(&fixture, &mut caller.pair, caller.root, parent_run).await;
    caller.pair.child.start_applied(&fixture, &large).await;
    let large_work = Work::of(&caller.pair.child, &large);
    let uploaded = fixture
        .gql()
        .query(
            &runner_access(),
            gesture::UPLOAD_RESULT,
            json!({ "input": { "work": large_work.wire(), "answer": gesture::content(&"x".repeat(600_000), &[]) } }),
        )
        .await;
    gesture::accepted(&uploaded, "jobsUploadResult");
    caller.pair.child.complete_run(&large).await;
    let both = wait_for_job_notice(
        &fixture,
        &caller.work,
        None,
        "RUN_COMPLETED",
        Some(large_work.job_id),
    )
    .await;
    let reference_only = both
        .iter()
        .find(|item| item["jobId"] == json!(large_work.job_id.to_string()))
        .expect("the notice of the large call");
    assert!(
        reference_only["answer"].is_null(),
        "above the bound, only a reference"
    );
    assert!(reference_only["resultId"].is_string());

    let doomed = call_child(&fixture, &mut caller.pair, caller.root, parent_run).await;
    caller.pair.child.start_applied(&fixture, &doomed).await;
    caller
        .pair
        .child
        .fail_run(&doomed, "PERMANENT", "boom", None)
        .await;
    let failed = wait_for_notice(&fixture, &caller.work, None, "JOB_FAILED").await;
    assert!(
        failed.iter().any(|item| item["noticeType"] == "JOB_FAILED"
            && item["failureReport"]["reason_code"] == "boom")
    );
    durable.close().await;
    fixture.shutdown().await;
}
