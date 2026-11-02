mod support;

use chrono::{Duration, Utc};
use serde_json::json;
use support::db::Durable;
use support::erasure::{assert_request_data_erased, assert_request_data_held};
use support::fixture::{JobsFixture, runner_access};
use support::gesture::{self, Mail};
use support::lineage::act;
use support::restart::{retry_manually, running_child};
use support::runner;
use support::scrub::{age_tree, assert_not_scrubbed_for, await_scrubbed};
use support::scrub_scene::{ENDED_RUN, end_run_and_finish, live_root, long_runs};
use support::{LONG, QUIET, gql, wire};
use uuid::Uuid;

const ROOT_OF_JOB: &str = "SELECT count(*) FROM jobs WHERE id = $1 AND root_job_id IS NOT NULL";
const IN_TREE: &str = "SELECT count(*) FROM jobs WHERE id = $1 AND root_job_id = $2";

async fn assert_in_tree(durable: &Durable, job: Uuid, root: Uuid) {
    let found: i64 = sqlx::query_scalar(IN_TREE)
        .bind(job)
        .bind(root)
        .fetch_one(durable.pool())
        .await
        .unwrap();
    assert_eq!(found, 1, "{job} belongs to the Tree of {root}");
    durable
        .assert_count(ROOT_OF_JOB, job, 1, "the Job names its Tree")
        .await;
}

#[tokio::test]
async fn a_manual_retry_puts_the_clock_of_its_tree_back_to_zero() {
    let fixture = JobsFixture::start_with(long_runs()).await;
    let durable = Durable::open(&fixture.app_url()).await;
    let live = live_root(&fixture, "clock-retry", 1).await;
    live.runner
        .fail_run(&live.trigger, "PERMANENT", "provider_refused", None)
        .await;
    gql::wait_for_status(&fixture.gql(), fixture.admin(), live.root, "FAILED", LONG).await;
    age_tree(&durable, live.root, 29).await;
    assert_not_scrubbed_for(&durable, live.root, QUIET, "29 days of activity").await;

    let successor = retry_manually(&fixture, live.root).await;
    let mut runner = live.runner;
    let trigger = runner.next_trigger(LONG).await;
    assert_eq!(runner::job_id(&trigger), successor);
    runner.start_applied(&fixture, &trigger).await;
    assert_in_tree(&durable, successor, live.root).await;

    age_tree(&durable, live.root, 2).await;
    assert_not_scrubbed_for(&durable, live.root, QUIET, "the successor is live").await;
    assert_request_data_held(&fixture, &durable, live.root, "the failed Initial Job").await;

    runner.complete_run(&trigger).await;
    durable
        .await_count(ENDED_RUN, runner::run_id(&trigger), 1, "the successor ends")
        .await;
    producer_finish(&fixture, successor).await;
    gql::wait_for_status(
        &fixture.gql(),
        fixture.admin(),
        successor,
        "COMPLETED",
        LONG,
    )
    .await;
    assert_not_scrubbed_for(&durable, live.root, QUIET, "the successor ended just now").await;
    age_tree(&durable, live.root, 29).await;
    assert_not_scrubbed_for(&durable, live.root, QUIET, "29 days after its end").await;
    age_tree(&durable, live.root, 2).await;
    await_scrubbed(&durable, live.root, "31 days after its end").await;
    assert_request_data_erased(&fixture, &durable, successor, "the successor").await;
    durable.close().await;
    fixture.shutdown().await;
}

async fn producer_finish(fixture: &JobsFixture, job: Uuid) {
    support::producer::Producer::new(fixture.fabric(), "projects")
        .finish_v2(job)
        .await;
}

#[tokio::test]
async fn a_restart_job_keeps_its_tree_alive_until_it_and_the_run_that_woke_it_have_ended() {
    let fixture = JobsFixture::start_with(long_runs()).await;
    let durable = Durable::open(&fixture.app_url()).await;
    let mut scene = running_child(&fixture, "clock-restart").await;
    scene.finish_child(&durable).await;
    age_tree(&durable, scene.root, 29).await;

    let mail = Mail::new(scene.sender, &[scene.receiver]);
    gesture::accepted(
        &gesture::send_mail(&fixture.gql(), &runner_access(), &scene.parent_work, &mail).await,
        "jobsSendMail",
    );
    let restart = scene.pair.child.next_trigger(LONG).await;
    let restart_job = runner::job_id(&restart);
    scene.pair.child.start_applied(&fixture, &restart).await;
    assert_in_tree(&durable, restart_job, scene.root).await;

    age_tree(&durable, scene.root, 2).await;
    assert_not_scrubbed_for(&durable, scene.root, QUIET, "the Run and the restart live").await;

    scene.pair.child.complete_run(&restart).await;
    durable
        .await_count(ENDED_RUN, runner::run_id(&restart), 1, "the restart ends")
        .await;
    for job in [restart_job, scene.child_work.job_id] {
        act(&fixture, wire::VERB_FINISH, job, &scene.parent_work).await;
        gql::wait_for_status(&fixture.gql(), fixture.admin(), job, "COMPLETED", LONG).await;
    }
    assert_not_scrubbed_for(&durable, scene.root, QUIET, "the Run of the root lives").await;
    scene.pair.parent.complete_run(&scene.parent_trigger).await;
    durable
        .await_count(ENDED_RUN, scene.parent_run(), 1, "the root Run ends")
        .await;
    producer_finish(&fixture, scene.root).await;
    gql::wait_for_status(
        &fixture.gql(),
        fixture.admin(),
        scene.root,
        "COMPLETED",
        LONG,
    )
    .await;
    assert_not_scrubbed_for(&durable, scene.root, QUIET, "the later end was just now").await;
    age_tree(&durable, scene.root, 29).await;
    assert_not_scrubbed_for(&durable, scene.root, QUIET, "29 days after the later end").await;
    age_tree(&durable, scene.root, 2).await;
    await_scrubbed(&durable, scene.root, "31 days after the later end").await;
    assert_request_data_erased(&fixture, &durable, restart_job, "the restart Job").await;
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn the_time_of_a_step_is_the_time_jobs_recorded_it_not_the_time_the_runner_reported() {
    let fixture = JobsFixture::start_with(long_runs()).await;
    let durable = Durable::open(&fixture.app_url()).await;
    let ahead = live_root(&fixture, "clock-step-ahead", 1).await;
    let behind = live_root(&fixture, "clock-step-behind", 1).await;
    for (live, reported) in [
        (&ahead, Utc::now() + Duration::days(100)),
        (&behind, Utc::now() - Duration::days(60)),
    ] {
        live.runner
            .publish_status(
                wire::FACT_STEP_STARTED,
                json!({
                    "run_id": live.work.run_id.to_string(),
                    "job_id": live.work.job_id.to_string(),
                    "index": 0,
                    "label": "a step",
                    "started_at": reported.to_rfc3339(),
                }),
            )
            .await;
        durable
            .await_count(
                "SELECT count(*) FROM steps WHERE run_id = $1",
                live.work.run_id,
                1,
                "the step is recorded",
            )
            .await;
        end_run_and_finish(&fixture, &durable, live).await;
    }
    assert_not_scrubbed_for(
        &durable,
        behind.root,
        QUIET,
        "the step reported in the past",
    )
    .await;
    age_tree(&durable, ahead.root, 31).await;
    await_scrubbed(&durable, ahead.root, "the step reported in the future").await;
    durable.close().await;
    fixture.shutdown().await;
}
