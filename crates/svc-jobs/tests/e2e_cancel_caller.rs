mod support;

use support::call_notices::wait_for_job_notice;
use support::cancel_chain::{
    CANCEL_NOTICES_AT, CANCEL_NOTICES_OF_RUN, REMOVED_RUN, WAKES_OF_RUN, chain,
};
use support::db::Durable;
use support::fixture::JobsFixture;
use support::lineage::act;
use support::{LONG, QUIET, gql, runner, wire};

fn cancel_notices(items: &[serde_json::Value]) -> Vec<&serde_json::Value> {
    items
        .iter()
        .filter(|item| item["noticeType"] == "JOB_CANCELLED")
        .collect()
}

#[tokio::test]
async fn the_cancel_of_a_job_above_a_finished_background_caller_leaves_it_no_notice_and_no_restart()
{
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let mut chain = support::cancel_chain::chain(&fixture, "above-finished", "BACKGROUND").await;
    chain.finish_b(&durable).await;
    chain.finish_a(&durable).await;

    let rings_before = [
        durable.rings_for(chain.b_work.run_id, None).await,
        durable.rings_for(chain.a_work.run_id, None).await,
    ];

    chain.cancel_as_administrator(&fixture, chain.a_job).await;
    chain
        .await_cancelled(&fixture, &[chain.a_job, chain.b_job, chain.c_job])
        .await;

    chain.b.expect_no_trigger(QUIET).await;
    chain.a.expect_no_trigger(QUIET).await;
    durable
        .assert_count(
            REMOVED_RUN,
            chain.b_work.run_id,
            1,
            "the completed Run of B is marked removed from the directory",
        )
        .await;
    let late = support::surface::register(
        fixture.nats(),
        &chain.b,
        chain.b_work.run_id,
        &[support::surface::Agent::with_inbox()],
    )
    .await;
    assert_eq!(support::surface::reason(&late), "run_removed");
    for ((run, address, what), before) in [
        (chain.b_work.run_id, chain.b_address, "B"),
        (chain.a_work.run_id, chain.a_address, "A"),
    ]
    .into_iter()
    .zip(rings_before)
    {
        durable
            .assert_count(
                CANCEL_NOTICES_OF_RUN,
                run,
                0,
                &format!("no cancel notice stays for the Run of {what}"),
            )
            .await;
        durable
            .assert_count(
                CANCEL_NOTICES_AT,
                address,
                0,
                &format!("no cancel notice stays at the address of {what}"),
            )
            .await;
        durable
            .assert_count(
                WAKES_OF_RUN,
                run,
                0,
                &format!("no restart is queued for {what}"),
            )
            .await;
        assert_eq!(
            durable.rings_for(run, None).await,
            before,
            "no doorbell is staged for a removed Caller {what}"
        );
    }
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn the_cancel_of_a_job_above_a_caller_whose_restart_runs_rings_nothing_for_the_removed_caller()
 {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let mut chain = support::cancel_chain::chain(&fixture, "above-restarted", "BACKGROUND").await;
    chain.finish_b(&durable).await;
    chain.finish_a(&durable).await;
    let restart = chain.restart_b(&fixture).await;
    let restart_run = runner::run_id(&restart);
    let before = durable.rings_for(restart_run, None).await;

    chain.cancel_as_administrator(&fixture, chain.a_job).await;
    chain
        .await_cancelled(
            &fixture,
            &[
                chain.a_job,
                chain.b_job,
                chain.c_job,
                runner::job_id(&restart),
            ],
        )
        .await;

    chain.b.expect_no_trigger(QUIET).await;
    assert_eq!(
        durable.rings_for(restart_run, None).await,
        before,
        "the live restart Run that acts for the removed Caller is rung for nothing the cancel made"
    );
    durable
        .assert_count(
            CANCEL_NOTICES_AT,
            chain.b_address,
            0,
            "no cancel notice stays at the address of B",
        )
        .await;
    durable
        .assert_count(
            WAKES_OF_RUN,
            chain.b_work.run_id,
            0,
            "no restart is queued for B",
        )
        .await;
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn the_cancel_of_a_job_above_a_sync_caller_and_its_called_job_leaves_no_notice() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let mut chain = support::cancel_chain::chain(&fixture, "above-sync", "SYNC").await;
    chain.finish_a(&durable).await;
    let before = durable.rings_for(chain.b_work.run_id, None).await;

    chain.cancel_as_administrator(&fixture, chain.a_job).await;
    chain
        .await_cancelled(&fixture, &[chain.a_job, chain.b_job, chain.c_job])
        .await;

    durable
        .assert_count(
            CANCEL_NOTICES_OF_RUN,
            chain.b_work.run_id,
            0,
            "no notice stays for the Run of B",
        )
        .await;
    assert_eq!(
        durable.rings_for(chain.b_work.run_id, None).await,
        before,
        "no doorbell is staged for the removed Caller"
    );
    chain.b.expect_no_trigger(QUIET).await;
    durable.close().await;
    fixture.shutdown().await;
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
    let restarted = support::gesture::Work::of(&chain.b, &restart);
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
async fn the_called_runner_that_cancels_itself_and_everything_below_tells_its_caller() {
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
            CANCEL_NOTICES_OF_RUN,
            chain.b_work.run_id,
            0,
            "B keeps no notice of C",
        )
        .await;
    durable
        .assert_count(CANCEL_NOTICES_AT, chain.b_address, 0, "nor at its address")
        .await;
    assert_eq!(
        gql::status_of(&fixture.gql(), fixture.admin(), chain.a_job).await,
        "IN_PROGRESS"
    );
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn the_cancel_of_the_restart_job_of_a_caller_alone_leaves_the_caller_no_notice_and_no_restart()
 {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let mut chain = chain(&fixture, "restart-alone", "BACKGROUND").await;
    chain.finish_b(&durable).await;
    let restart = chain.restart_b(&fixture).await;
    let restart_job = runner::job_id(&restart);
    let restart_run = runner::run_id(&restart);
    let before = durable.rings_for(restart_run, None).await;

    chain.cancel_as_administrator(&fixture, restart_job).await;
    chain
        .await_cancelled(&fixture, &[restart_job, chain.c_job])
        .await;

    chain.b.expect_no_trigger(QUIET).await;
    durable
        .assert_count(
            CANCEL_NOTICES_AT,
            chain.b_address,
            0,
            "no cancel notice stays at the address of B",
        )
        .await;
    durable
        .assert_count(
            CANCEL_NOTICES_OF_RUN,
            chain.b_work.run_id,
            0,
            "no cancel notice stays for the first Run of B",
        )
        .await;
    durable
        .assert_count(
            WAKES_OF_RUN,
            chain.b_work.run_id,
            0,
            "no restart is queued for B",
        )
        .await;
    assert_eq!(durable.rings_for(restart_run, None).await, before);
    durable.close().await;
    fixture.shutdown().await;
}
