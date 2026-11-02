mod support;

use support::call_notices::wait_for_job_notice;
use support::cancel_chain::{NOTICES_AT, NOTICES_OF_RUN, chain, chain_between};
use support::db::Durable;
use support::fixture::JobsFixture;
use support::lineage::act;
use support::{LONG, gesture, gql, wire};

fn cancel_notices(items: &[serde_json::Value]) -> Vec<&serde_json::Value> {
    items
        .iter()
        .filter(|item| item["noticeType"] == "JOB_CANCELLED")
        .collect()
}

#[tokio::test]
async fn the_cancel_of_the_called_job_alone_tells_a_background_caller_that_stays() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let chain = chain(&fixture, "called-alone-live", "BACKGROUND").await;

    chain.cancel_as_administrator(&fixture, chain.c_job).await;
    let seen = wait_for_job_notice(
        &fixture,
        &chain.b_work,
        Some(chain.b_address),
        "JOB_CANCELLED",
        Some(chain.c_job),
    )
    .await;
    assert_eq!(cancel_notices(&seen).len(), 1, "{seen:?}");
    assert_eq!(
        gql::status_of(&fixture.gql(), fixture.admin(), chain.b_job).await,
        "IN_PROGRESS"
    );
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn the_cancel_of_the_called_job_alone_restarts_a_finished_background_caller() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let mut chain = chain(&fixture, "called-alone-finished", "BACKGROUND").await;
    chain.finish_b(&durable).await;

    chain.cancel_as_administrator(&fixture, chain.c_job).await;
    let restart = chain.b.next_trigger(LONG).await;
    assert_eq!(
        restart["restart"]["restart_of_run_id"],
        serde_json::json!(chain.b_work.run_id.to_string())
    );
    chain.b.start_applied(&fixture, &restart).await;
    let restarted = gesture::Work::of(&chain.b, &restart);
    wait_for_job_notice(
        &fixture,
        &restarted,
        Some(chain.b_address),
        "JOB_CANCELLED",
        Some(chain.c_job),
    )
    .await;
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn the_cancel_of_the_called_job_alone_tells_a_sync_caller_that_stays() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let chain = chain(&fixture, "called-alone-sync", "SYNC").await;

    chain.cancel_as_administrator(&fixture, chain.c_job).await;
    let seen = wait_for_job_notice(
        &fixture,
        &chain.b_work,
        None,
        "JOB_CANCELLED",
        Some(chain.c_job),
    )
    .await;
    assert_eq!(cancel_notices(&seen).len(), 1, "{seen:?}");
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn the_called_runner_that_cancels_itself_and_everything_below_tells_its_background_caller() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let chain = chain(&fixture, "self-cancel", "BACKGROUND").await;

    act(&fixture, wire::VERB_CANCEL, chain.b_job, &chain.b_work).await;
    chain
        .await_cancelled(&fixture, &[chain.b_job, chain.c_job])
        .await;
    let seen = wait_for_job_notice(
        &fixture,
        &chain.a_work,
        Some(chain.a_address),
        "JOB_CANCELLED",
        Some(chain.b_job),
    )
    .await;
    assert_eq!(cancel_notices(&seen).len(), 1, "{seen:?}");
    durable
        .assert_count(
            NOTICES_OF_RUN,
            chain.b_work.run_id,
            0,
            "B keeps no notice of C",
        )
        .await;
    durable
        .assert_count(NOTICES_AT, chain.b_address, 0, "nor at its address")
        .await;
    assert_eq!(
        gql::status_of(&fixture.gql(), fixture.admin(), chain.a_job).await,
        "IN_PROGRESS"
    );
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn the_called_runner_that_cancels_itself_restarts_a_finished_background_caller_that_can_restart()
 {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let mut chain = chain(&fixture, "self-cancel-finished", "BACKGROUND").await;
    chain.finish_a(&durable).await;

    act(&fixture, wire::VERB_CANCEL, chain.b_job, &chain.b_work).await;
    chain
        .await_cancelled(&fixture, &[chain.b_job, chain.c_job])
        .await;
    let restart = chain.a.next_trigger(LONG).await;
    assert_eq!(
        restart["restart"]["restart_of_run_id"],
        serde_json::json!(chain.a_work.run_id.to_string())
    );
    chain.a.start_applied(&fixture, &restart).await;
    wait_for_job_notice(
        &fixture,
        &gesture::Work::of(&chain.a, &restart),
        Some(chain.a_address),
        "JOB_CANCELLED",
        Some(chain.b_job),
    )
    .await;
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn the_called_runner_that_cancels_itself_tells_its_sync_caller_in_its_run() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let chain = chain_between(&fixture, "self-cancel-sync", "SYNC", "BACKGROUND").await;

    act(&fixture, wire::VERB_CANCEL, chain.b_job, &chain.b_work).await;
    chain
        .await_cancelled(&fixture, &[chain.b_job, chain.c_job])
        .await;
    let seen = wait_for_job_notice(
        &fixture,
        &chain.a_work,
        None,
        "JOB_CANCELLED",
        Some(chain.b_job),
    )
    .await;
    assert_eq!(cancel_notices(&seen).len(), 1, "{seen:?}");
    durable
        .assert_count(
            NOTICES_OF_RUN,
            chain.b_work.run_id,
            0,
            "B keeps no notice of C",
        )
        .await;
    durable.close().await;
    fixture.shutdown().await;
}
