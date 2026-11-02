mod support;

use support::cancel_chain::{NOTICES_AT, NOTICES_OF_RUN, REMOVED_RUN, WAKES_OF_RUN, chain};
use support::db::Durable;
use support::events::EventLog;
use support::fixture::JobsFixture;
use support::removed_address::a_mail_to_is_refused_as_removed;
use support::{LONG, QUIET, runner, wire};

async fn each_job_is_announced_cancelled_once(events: &EventLog, jobs: &[uuid::Uuid]) {
    for job in jobs {
        events
            .expect_exactly(wire::FACT_CANCELLED, *job, 1, LONG)
            .await;
    }
}

#[tokio::test]
async fn the_cancel_of_a_job_above_a_finished_background_caller_leaves_it_no_notice_and_no_restart()
{
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let events = EventLog::open(fixture.fabric()).await;
    let mut chain = chain(&fixture, "above-finished", "BACKGROUND").await;
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

    each_job_is_announced_cancelled_once(&events, &[chain.a_job, chain.b_job, chain.c_job]).await;
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
    a_mail_to_is_refused_as_removed(&fixture, "above-finished-sender", chain.b_address).await;
    a_mail_to_is_refused_as_removed(&fixture, "above-finished-sender-a", chain.a_address).await;
    for ((run, address, what), before) in [
        (chain.b_work.run_id, chain.b_address, "B"),
        (chain.a_work.run_id, chain.a_address, "A"),
    ]
    .into_iter()
    .zip(rings_before)
    {
        durable
            .assert_count(
                NOTICES_OF_RUN,
                run,
                0,
                &format!("no notice stays for the Run of {what}"),
            )
            .await;
        durable
            .assert_count(
                NOTICES_AT,
                address,
                0,
                &format!("no notice stays at the address of {what}"),
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
        chain.assert_no_restart_beyond(&durable, run, 0).await;
    }
    events.stop().await;
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn the_cancel_of_a_job_above_a_caller_whose_restart_runs_rings_nothing_for_the_removed_caller()
 {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let mut chain = chain(&fixture, "above-restarted", "BACKGROUND").await;
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
            NOTICES_AT,
            chain.b_address,
            0,
            "no notice stays at the address of B",
        )
        .await;
    chain
        .assert_no_restart_beyond(&durable, chain.b_work.run_id, 1)
        .await;
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn the_cancel_of_a_job_above_a_sync_caller_and_its_called_job_leaves_no_notice() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let events = EventLog::open(fixture.fabric()).await;
    let mut chain = chain(&fixture, "above-sync", "SYNC").await;
    chain.finish_a(&durable).await;
    let before = durable.rings_for(chain.b_work.run_id, None).await;

    chain.cancel_as_administrator(&fixture, chain.a_job).await;
    chain
        .await_cancelled(&fixture, &[chain.a_job, chain.b_job, chain.c_job])
        .await;

    each_job_is_announced_cancelled_once(&events, &[chain.a_job, chain.b_job, chain.c_job]).await;
    durable
        .assert_count(
            NOTICES_OF_RUN,
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
    chain
        .assert_no_restart_beyond(&durable, chain.b_work.run_id, 0)
        .await;
    events.stop().await;
    durable.close().await;
    fixture.shutdown().await;
}
