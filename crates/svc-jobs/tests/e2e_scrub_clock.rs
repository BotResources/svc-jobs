mod support;

use support::db::Durable;
use support::erasure::assert_request_data_held;
use support::erasure_scene::child_body;
use support::fixture::JobsFixture;
use support::lineage::{create, live_runner};
use support::runner;
use support::scrub::{age_tree, assert_not_scrubbed_for, await_scrubbed};
use support::scrub_scene::{ENDED_RUN, end_run_and_finish, live_root, long_runs};
use support::{LONG, QUIET, gql, wire};
use uuid::Uuid;

const SCHEDULES_OF_JOB: &str = "SELECT count(*) FROM run_retry_schedules \
     WHERE due_at > now() + interval '39 days' \
       AND failed_run_id IN (SELECT id FROM runs WHERE job_id = $1)";

async fn cancel(fixture: &JobsFixture, job: Uuid) {
    gql::cancel_job(&fixture.gql(), fixture.admin(), Uuid::now_v7(), job).await;
    gql::wait_for_status(&fixture.gql(), fixture.admin(), job, "CANCELLED", LONG).await;
}

#[tokio::test]
async fn the_clock_of_a_tree_stops_while_a_job_waits_and_runs_again_from_its_end() {
    let fixture = JobsFixture::start_with(long_runs()).await;
    let durable = Durable::open(&fixture.app_url()).await;
    let live = live_root(&fixture, "clock-waits", 1).await;
    let unavailable = wire::unique_runner_type("clock-none");
    let b = create(
        &fixture,
        &live.work,
        &unavailable,
        live.root,
        child_body("SYNC"),
    )
    .await;
    gql::wait_for_status(&fixture.gql(), fixture.admin(), b, "PENDING", LONG).await;
    end_run_and_finish(&fixture, &durable, &live).await;

    age_tree(&durable, live.root, 60).await;
    assert_not_scrubbed_for(&durable, live.root, QUIET, "B waits").await;
    assert_request_data_held(&fixture, &durable, live.root, "the Initial Job").await;

    cancel(&fixture, b).await;
    assert_not_scrubbed_for(&durable, live.root, QUIET, "B ended just now").await;
    age_tree(&durable, live.root, 29).await;
    assert_not_scrubbed_for(&durable, live.root, QUIET, "29 days after the end of B").await;
    age_tree(&durable, live.root, 2).await;
    await_scrubbed(&durable, live.root, "31 days after the end of B").await;
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn the_clock_of_a_tree_stops_while_a_run_lives_and_runs_again_from_its_end() {
    let fixture = JobsFixture::start_with(long_runs()).await;
    let durable = Durable::open(&fixture.app_url()).await;
    let live = live_root(&fixture, "clock-lives", 1).await;

    age_tree(&durable, live.root, 60).await;
    assert_not_scrubbed_for(&durable, live.root, QUIET, "the Run lives").await;
    assert_request_data_held(&fixture, &durable, live.root, "the Initial Job").await;

    end_run_and_finish(&fixture, &durable, &live).await;
    assert_not_scrubbed_for(&durable, live.root, QUIET, "the Run ended just now").await;
    age_tree(&durable, live.root, 29).await;
    assert_not_scrubbed_for(&durable, live.root, QUIET, "29 days after the end").await;
    age_tree(&durable, live.root, 2).await;
    await_scrubbed(&durable, live.root, "31 days after the end").await;
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn the_clock_of_a_tree_stops_while_a_job_is_about_to_retry_and_runs_again_from_its_end() {
    let fixture = JobsFixture::start_with(long_runs()).await;
    let durable = Durable::open(&fixture.app_url()).await;
    let live = live_root(&fixture, "clock-retries", 2).await;
    live.runner
        .fail_run(
            &live.trigger,
            "TRANSIENT",
            "provider_timeout",
            Some(40 * 86_400),
        )
        .await;
    durable
        .await_count(SCHEDULES_OF_JOB, live.root, 1, "the retry waits")
        .await;

    age_tree(&durable, live.root, 60).await;
    assert_not_scrubbed_for(&durable, live.root, QUIET, "a retry is due in 40 days").await;
    assert_request_data_held(&fixture, &durable, live.root, "the Initial Job").await;

    cancel(&fixture, live.root).await;
    assert_not_scrubbed_for(&durable, live.root, QUIET, "the Job ended just now").await;
    age_tree(&durable, live.root, 29).await;
    assert_not_scrubbed_for(&durable, live.root, QUIET, "29 days after the end").await;
    age_tree(&durable, live.root, 2).await;
    await_scrubbed(&durable, live.root, "31 days after the end").await;
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_run_that_was_dispatched_and_never_started_does_not_hold_the_tree_when_its_job_is_cancelled()
 {
    let fixture = JobsFixture::start_with(long_runs()).await;
    let durable = Durable::open(&fixture.app_url()).await;
    let live = live_root(&fixture, "clock-dispatched", 1).await;
    let mut dispatched = live_runner(&fixture, "clock-dispatched-b").await;
    let b = create(
        &fixture,
        &live.work,
        &dispatched.runner_type,
        live.root,
        child_body("SYNC"),
    )
    .await;
    let trigger = dispatched.next_trigger(LONG).await;
    assert_eq!(runner::job_id(&trigger), b);
    cancel(&fixture, b).await;
    durable
        .await_count(ENDED_RUN, runner::run_id(&trigger), 1, "the Run of B ends")
        .await;
    end_run_and_finish(&fixture, &durable, &live).await;
    age_tree(&durable, live.root, 31).await;
    await_scrubbed(&durable, live.root, "no Run of the Tree is live").await;
    durable.close().await;
    fixture.shutdown().await;
}
