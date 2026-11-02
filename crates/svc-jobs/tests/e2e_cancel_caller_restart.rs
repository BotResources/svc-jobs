mod support;

use support::cancel_chain::{Chain, NOTICES_AT, NOTICES_OF_RUN, chain, chain_between};
use support::db::Durable;
use support::fixture::JobsFixture;
use support::restart::FINISHED_OF_RUN;
use support::surface::Agent;
use support::{LONG, QUIET, runner};

const CANCEL_NOTICES_AT: &str = "SELECT count(*) FROM inbox_items \
     WHERE address = $1 AND notice ->> 'type' = 'JOB_CANCELLED'";
const CANCEL_NOTICES_OF_RUN: &str = "SELECT count(*) FROM inbox_items \
     WHERE run_id = $1 AND address IS NULL AND notice ->> 'type' = 'JOB_CANCELLED'";

async fn cancel_the_restart_job_alone(
    chain: &mut Chain<'_>,
    fixture: &JobsFixture,
    agents: Vec<Agent>,
) {
    let restart = chain.restart_b_registering(fixture, agents).await;
    let restart_job = runner::job_id(&restart);
    chain.cancel_as_administrator(fixture, restart_job).await;
    chain
        .await_cancelled(fixture, &[restart_job, chain.c_job])
        .await;
}

async fn the_caller_is_restarted_again_for_its_notice(chain: &mut Chain<'_>) {
    let second_restart = chain.b.next_trigger(LONG).await;
    assert_eq!(
        second_restart["restart"]["restart_of_run_id"],
        serde_json::json!(chain.b_work.run_id.to_string())
    );
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
            NOTICES_AT,
            chain.b_address,
            0,
            "no notice stays at the address of B",
        )
        .await;
    durable
        .assert_count(
            NOTICES_OF_RUN,
            chain.b_work.run_id,
            0,
            "no notice stays for the first Run of B",
        )
        .await;
    chain
        .assert_no_restart_beyond(&durable, chain.b_work.run_id, 1)
        .await;
    assert_eq!(durable.rings_for(restart_run, None).await, before);
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn the_cancel_of_the_restart_job_alone_leaves_a_sync_caller_whose_agents_it_took_no_notice() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let mut chain = chain_between(&fixture, "restart-alone-sync", "BACKGROUND", "SYNC").await;
    chain.finish_b(&durable).await;
    let taken_over = Agent::with_inbox().with_id(chain.b_address);

    cancel_the_restart_job_alone(&mut chain, &fixture, vec![taken_over]).await;

    chain.b.expect_no_trigger(QUIET).await;
    durable
        .assert_count(
            NOTICES_OF_RUN,
            chain.b_work.run_id,
            0,
            "no notice of C stays for the first Run of B",
        )
        .await;
    chain
        .assert_no_restart_beyond(&durable, chain.b_work.run_id, 1)
        .await;
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn the_cancel_of_a_restart_job_that_took_another_agent_leaves_the_background_caller_that_stays_its_notice()
 {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let mut chain = chain(&fixture, "restart-partial", "BACKGROUND").await;
    chain.finish_b(&durable).await;

    cancel_the_restart_job_alone(&mut chain, &fixture, vec![Agent::with_inbox()]).await;

    durable
        .assert_count(
            FINISHED_OF_RUN,
            chain.b_work.run_id,
            1,
            "the Caller agent stays finished at its first Run",
        )
        .await;
    durable
        .assert_count(
            CANCEL_NOTICES_AT,
            chain.b_address,
            1,
            "a Caller that stays keeps the cancel notice of the called Job",
        )
        .await;
    the_caller_is_restarted_again_for_its_notice(&mut chain).await;
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn the_cancel_of_a_restart_job_that_took_another_agent_leaves_the_sync_caller_that_stays_its_notice()
 {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let mut chain = chain_between(&fixture, "restart-partial-sync", "BACKGROUND", "SYNC").await;
    chain.finish_b(&durable).await;

    cancel_the_restart_job_alone(&mut chain, &fixture, vec![Agent::with_inbox()]).await;

    durable
        .assert_count(
            CANCEL_NOTICES_OF_RUN,
            chain.b_work.run_id,
            1,
            "the first Run of B keeps the cancel notice: its agent stayed",
        )
        .await;
    the_caller_is_restarted_again_for_its_notice(&mut chain).await;
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn the_cancel_of_a_restart_job_that_took_no_agent_leaves_the_sync_caller_its_notice() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let mut chain = chain_between(&fixture, "restart-bare-sync", "BACKGROUND", "SYNC").await;
    chain.finish_b(&durable).await;

    cancel_the_restart_job_alone(&mut chain, &fixture, Vec::new()).await;

    durable
        .assert_count(
            CANCEL_NOTICES_OF_RUN,
            chain.b_work.run_id,
            1,
            "the first Run of B keeps the cancel notice: its agent stayed",
        )
        .await;
    the_caller_is_restarted_again_for_its_notice(&mut chain).await;
    durable.close().await;
    fixture.shutdown().await;
}
