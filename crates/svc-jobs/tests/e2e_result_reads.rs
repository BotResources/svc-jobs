mod support;

use serde_json::{Value, json};
use support::LONG;
use support::db::Durable;
use support::fixture::{JobsFixture, runner_access};
use support::gesture::{self, Mail, Work};
use support::restart::{
    FINISHED_OF_RUN, read_result, result_for, running_child, save_state, upload_result,
};
use support::runner;
use support::tree::call_child;
use uuid::Uuid;

const RESULTS_WITH_ID: &str = "SELECT count(*) FROM job_results WHERE result_id = $1";

async fn read_once(fixture: &JobsFixture, work: &Work, result: Uuid) -> Value {
    fixture
        .gql()
        .query(
            &runner_access(),
            gesture::READ_RESULT,
            json!({ "input": { "work": work.wire(), "resultId": result.to_string(), "keep": false } }),
        )
        .await
}

#[tokio::test]
async fn a_result_read_by_reference_is_erased_and_one_read_with_keep_is_read_again_by_the_restart_run()
 {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let mut scene = running_child(&fixture, "result-reads").await;
    let parent_run = scene.parent_run();
    upload_result(&fixture, &scene.child_work, "erased on read").await;
    let erased = result_for(&durable, scene.child_work.job_id).await;
    let kept_trigger = call_child(&fixture, &mut scene.pair, scene.root, parent_run).await;
    scene
        .pair
        .child
        .start_applied(&fixture, &kept_trigger)
        .await;
    let kept_work = Work::of(&scene.pair.child, &kept_trigger);
    upload_result(&fixture, &kept_work, "kept by its reader").await;
    let kept = result_for(&durable, kept_work.job_id).await;

    let first = read_once(&fixture, &scene.parent_work, erased).await;
    assert_eq!(
        gesture::field(&first, "jobsReadResult")["content"]["message"],
        "erased on read"
    );
    durable
        .assert_count(
            RESULTS_WITH_ID,
            erased,
            0,
            "a read by reference erases the Result",
        )
        .await;
    let again = read_once(&fixture, &scene.parent_work, erased).await;
    assert_eq!(gesture::refusal(&again, "jobsReadResult"), "result_unknown");

    for _ in 0..2 {
        let read = read_result(&fixture, &scene.parent_work, kept).await;
        assert_eq!(
            gesture::field(&read, "jobsReadResult")["content"]["message"],
            "kept by its reader",
            "a read with keep leaves the Result for a second read"
        );
    }

    save_state(&fixture, &scene.parent_work, json!({ "turn": 2 })).await;
    scene.pair.parent.complete_run(&scene.parent_trigger).await;
    durable
        .await_count(
            FINISHED_OF_RUN,
            parent_run,
            2,
            "the agents of A's Run finish",
        )
        .await;
    gesture::accepted(
        &gesture::send_mail(
            &fixture.gql(),
            &runner_access(),
            &scene.child_work,
            &Mail::new(scene.receiver, &[scene.sender]),
        )
        .await,
        "jobsSendMail",
    );
    let restart = scene.pair.parent.next_trigger(LONG).await;
    assert_ne!(runner::job_id(&restart), scene.root);
    scene.pair.parent.start_applied(&fixture, &restart).await;
    let by_restart = read_result(&fixture, &Work::of(&scene.pair.parent, &restart), kept).await;
    assert_eq!(
        gesture::field(&by_restart, "jobsReadResult")["content"]["message"],
        "kept by its reader",
        "the Run of A's restart Job reads the kept Result again"
    );
    durable
        .assert_count(RESULTS_WITH_ID, kept, 1, "a read with keep keeps it")
        .await;
    durable.close().await;
    fixture.shutdown().await;
}
