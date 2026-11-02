mod support;

use support::auto_finish::{
    background, becomes_completed, complete, mail_the_answer, mark_the_mail, mark_the_notice,
    one_completion_each, stays, sync_call,
};
use support::call_notices::caller;
use support::db::Durable;
use support::events::EventLog;
use support::fixture::{JobsFixture, Knobs};
use support::lineage::act;
use support::{LONG, gql, wire};

#[tokio::test]
async fn jobs_finishes_the_tool_node_and_background_calls_whose_answer_the_caller_got() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let events = EventLog::open_v2(fixture.fabric()).await;
    let mut caller = caller(&fixture, "auto-finish").await;

    let tool = sync_call(&fixture, &mut caller).await;
    let node = sync_call(&fixture, &mut caller).await;
    let background = background(&fixture, &mut caller).await;
    let answer = mail_the_answer(&fixture, &caller, &background).await;
    for called in [&tool, &node, &background] {
        complete(&caller, called).await;
    }
    mark_the_notice(&fixture, &caller, &tool).await;
    mark_the_notice(&fixture, &caller, &node).await;
    mark_the_mail(&fixture, &caller, answer).await;

    let jobs = [tool.job, node.job, background.job];
    stays(&fixture, &jobs, "IN_PROGRESS").await;

    caller.pair.parent.complete_run(&caller.trigger).await;
    becomes_completed(&fixture, &jobs).await;
    for job in jobs {
        events
            .expect_exactly(wire::FACT_COMPLETED, job, 1, LONG)
            .await;
    }
    one_completion_each(&durable, &jobs).await;
    events.stop().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn jobs_touches_nothing_while_one_of_the_three_conditions_is_missing() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let events = EventLog::open_v2(fixture.fabric()).await;
    let mut caller = caller(&fixture, "auto-finish-untouched").await;

    let finished = sync_call(&fixture, &mut caller).await;
    complete(&caller, &finished).await;
    mark_the_notice(&fixture, &caller, &finished).await;

    let still_running = sync_call(&fixture, &mut caller).await;
    let unmarked = sync_call(&fixture, &mut caller).await;
    complete(&caller, &unmarked).await;
    support::call_notices::wait_for_job_notice(
        &fixture,
        &caller.work,
        None,
        "RUN_COMPLETED",
        Some(unmarked.job),
    )
    .await;
    let unread_answer = background(&fixture, &mut caller).await;
    mail_the_answer(&fixture, &caller, &unread_answer).await;
    complete(&caller, &unread_answer).await;
    let no_mail = background(&fixture, &mut caller).await;
    complete(&caller, &no_mail).await;

    stays(&fixture, &[finished.job], "IN_PROGRESS").await;
    caller.pair.parent.complete_run(&caller.trigger).await;
    becomes_completed(&fixture, &[finished.job]).await;

    let untouched = [
        still_running.job,
        unmarked.job,
        unread_answer.job,
        no_mail.job,
        caller.root,
    ];
    stays(&fixture, &untouched, "IN_PROGRESS").await;
    for job in untouched {
        events
            .expect_none(wire::FACT_COMPLETED, job, support::SHORT)
            .await;
        durable
            .assert_count(
                support::auto_finish::RESOLUTIONS_OF_JOB,
                job,
                0,
                "the Job was not resolved",
            )
            .await;
    }
    events.stop().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_caller_that_fails_finishes_nothing() {
    let fixture = JobsFixture::start().await;
    let events = EventLog::open_v2(fixture.fabric()).await;
    let mut caller = caller(&fixture, "auto-finish-failed").await;
    let tool = sync_call(&fixture, &mut caller).await;
    complete(&caller, &tool).await;
    mark_the_notice(&fixture, &caller, &tool).await;

    caller
        .pair
        .parent
        .fail_run(&caller.trigger, "PERMANENT", "provider_refused", None)
        .await;

    gql::wait_for_status(&fixture.gql(), fixture.admin(), tool.job, "CANCELLED", LONG).await;
    events
        .expect_none(wire::FACT_COMPLETED, tool.job, support::SHORT)
        .await;
    events.stop().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_job_that_the_caller_finished_first_is_not_finished_again() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let events = EventLog::open_v2(fixture.fabric()).await;
    let mut caller = caller(&fixture, "auto-finish-first").await;
    let tool = sync_call(&fixture, &mut caller).await;
    let other = sync_call(&fixture, &mut caller).await;
    for called in [&tool, &other] {
        complete(&caller, called).await;
        mark_the_notice(&fixture, &caller, called).await;
    }
    act(&fixture, wire::VERB_FINISH, tool.job, &caller.work).await;
    becomes_completed(&fixture, &[tool.job]).await;

    caller.pair.parent.complete_run(&caller.trigger).await;

    becomes_completed(&fixture, &[other.job]).await;
    stays(&fixture, &[tool.job], "COMPLETED").await;
    one_completion_each(&durable, &[tool.job, other.job]).await;
    events
        .expect_exactly(wire::FACT_COMPLETED, tool.job, 1, LONG)
        .await;
    events.stop().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn the_end_of_the_caller_run_finishes_the_called_job_without_waiting_for_a_sweep() {
    let fixture = JobsFixture::start_with(Knobs {
        backstop_interval_seconds: 600,
        ..Knobs::default()
    })
    .await;
    let mut caller = caller(&fixture, "auto-finish-at-once").await;
    let called = sync_call(&fixture, &mut caller).await;
    complete(&caller, &called).await;
    mark_the_notice(&fixture, &caller, &called).await;
    stays(&fixture, &[called.job], "IN_PROGRESS").await;

    caller.pair.parent.complete_run(&caller.trigger).await;

    becomes_completed(&fixture, &[called.job]).await;
    fixture.shutdown().await;
}
