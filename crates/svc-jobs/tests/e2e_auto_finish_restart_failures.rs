mod support;

use serde_json::json;
use support::auto_finish::{
    Restartable, becomes_completed, complete, mark_the_mail, mark_the_mail_as, one_completion_each,
    reply_to, restartable, send_answer, send_answer_as, start_restart, started_restart, stays,
    unread_mails,
};
use support::call_notices::{Caller, caller};
use support::db::Durable;
use support::fixture::{JobsFixture, Knobs};
use support::restart::{RESTARTS_OF_RUN, save_state};
use support::{LONG, gql};
use uuid::Uuid;

async fn restart_job_of(durable: &Durable, run: Uuid) -> Uuid {
    sqlx::query_scalar("SELECT job_id::uuid FROM restart_origins WHERE restarts_run_id = $1")
        .bind(run)
        .fetch_one(durable.pool())
        .await
        .expect("the restart job exists")
}

async fn answered_and_read(fixture: &JobsFixture, caller: &mut Caller<'_>) -> Restartable {
    let m = restartable(fixture, caller, true).await;
    let answer = send_answer(fixture, caller, &m.called, m.agent).await;
    complete(caller, &m.called).await;
    mark_the_mail(fixture, caller, answer).await;
    m
}

async fn replied_to(
    fixture: &JobsFixture,
    durable: &Durable,
    caller: &Caller<'_>,
    m: &Restartable,
) -> Uuid {
    reply_to(fixture, caller, m.agent).await;
    durable
        .await_count(
            RESTARTS_OF_RUN,
            m.called.work.run_id,
            1,
            "the reply restarts",
        )
        .await;
    restart_job_of(durable, m.called.work.run_id).await
}

#[tokio::test]
async fn a_restart_job_that_fails_for_good_or_is_cancelled_alone_does_not_hold_the_finish() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let mut caller = caller(&fixture, "auto-restart-lost").await;
    let failing = answered_and_read(&fixture, &mut caller).await;
    let cancelled = answered_and_read(&fixture, &mut caller).await;
    let failing_job = replied_to(&fixture, &durable, &caller, &failing).await;
    let cancelled_job = replied_to(&fixture, &durable, &caller, &cancelled).await;

    caller.pair.parent.complete_run(&caller.trigger).await;
    stays(
        &fixture,
        &[failing.called.job, cancelled.called.job],
        "IN_PROGRESS",
    )
    .await;

    let mut triggers = vec![
        caller.pair.child.next_trigger(LONG).await,
        caller.pair.child.next_trigger(LONG).await,
    ];
    let ours = triggers
        .iter()
        .position(|trigger| support::runner::job_id(trigger) == failing_job)
        .expect("the trigger of the restart job that fails");
    let run = start_restart(
        &fixture,
        &caller.pair.child,
        triggers.remove(ours),
        failing.agent,
    )
    .await;
    caller
        .pair
        .child
        .fail_run(&run.trigger, "PERMANENT", "provider_refused", None)
        .await;
    becomes_completed(&fixture, &[failing.called.job]).await;
    assert_eq!(
        gql::status_of(&fixture.gql(), fixture.admin(), failing_job).await,
        "FAILED",
        "the restart job that failed for good stays failed"
    );
    stays(&fixture, &[cancelled.called.job], "IN_PROGRESS").await;

    let cancelling = Uuid::now_v7();
    let cancelled_answer =
        gql::cancel_job(&fixture.gql(), fixture.admin(), cancelling, cancelled_job).await;
    assert!(
        cancelled_answer["errors"].is_null(),
        "the administrator cancels the restart job alone: {cancelled_answer}"
    );
    becomes_completed(&fixture, &[cancelled.called.job]).await;
    assert_eq!(
        gql::status_of(&fixture.gql(), fixture.admin(), cancelled_job).await,
        "CANCELLED"
    );
    one_completion_each(&durable, &[failing.called.job, cancelled.called.job]).await;
    let metadata = durable
        .metadata_of_event("JobCompleted", cancelled.called.job)
        .await;
    assert_eq!(
        (
            metadata["actor_kind"].as_str(),
            metadata["actor_id"].as_str()
        ),
        (
            Some("service"),
            Some(svc_jobs::app::SERVICE_ACTOR.to_string().as_str())
        ),
        "the finish is attributed to the service account of Jobs, not to the administrator who cancelled"
    );
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_restart_run_of_the_caller_that_fails_after_reading_the_answer_leaves_the_job_open_until_the_timeout()
 {
    let fixture = JobsFixture::start_with(Knobs {
        inactivity_timeout_seconds: 12,
        ..Knobs::default()
    })
    .await;
    let mut caller = caller(&fixture, "auto-caller-fails").await;
    save_state(&fixture, &caller.work, json!({ "turn": 1 })).await;
    let called = support::auto_finish::background(&fixture, &mut caller).await;
    let answer = send_answer_with_agent(&fixture, &caller, &called).await;
    complete(&caller, &called).await;

    caller.pair.parent.complete_run(&caller.trigger).await;
    stays(&fixture, &[called.job], "IN_PROGRESS").await;

    let restart = started_restart(&fixture, &mut caller.pair.parent, caller.address).await;
    mark_the_mail_as(
        &fixture,
        &caller.pair.parent,
        &restart.work,
        caller.address,
        answer,
    )
    .await;
    caller
        .pair
        .parent
        .fail_run(&restart.trigger, "PERMANENT", "boom_after_reading", None)
        .await;

    assert_eq!(
        gql::status_of(&fixture.gql(), fixture.admin(), called.job).await,
        "IN_PROGRESS",
        "the called job is not cancelled: the restart run is not its Requester"
    );
    gql::wait_for_status(&fixture.gql(), fixture.admin(), called.job, "FAILED", LONG).await;
    let view = gql::job(&fixture.gql(), fixture.admin(), called.job).await;
    assert_eq!(view["resolution"]["failureCause"], "INACTIVITY_TIMEOUT");
    fixture.shutdown().await;
}

async fn send_answer_with_agent(
    fixture: &JobsFixture,
    caller: &Caller<'_>,
    called: &support::auto_finish::Called,
) -> Uuid {
    let agent = support::surface::Agent::with_inbox();
    let id = agent.agent_id;
    support::surface::register(
        fixture.nats(),
        &caller.pair.child,
        called.work.run_id,
        &[agent],
    )
    .await;
    send_answer_as(fixture, &called.work, id, caller.address).await
}

#[tokio::test]
async fn the_restart_run_of_the_caller_that_reads_the_answer_finishes_what_the_first_run_left() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let mut caller = caller(&fixture, "auto-restart-read").await;
    save_state(&fixture, &caller.work, json!({ "turn": 1 })).await;
    let called = support::auto_finish::background(&fixture, &mut caller).await;

    caller.pair.parent.complete_run(&caller.trigger).await;
    stays(&fixture, &[called.job], "IN_PROGRESS").await;

    let answer = send_answer_with_agent(&fixture, &caller, &called).await;
    complete(&caller, &called).await;
    let restart = started_restart(&fixture, &mut caller.pair.parent, caller.address).await;
    stays(&fixture, &[called.job], "IN_PROGRESS").await;
    let unread = unread_mails(&fixture, &restart.work, caller.address).await;
    assert_eq!(unread.len(), 1, "the restart run finds the answer unread");
    mark_the_mail_as(
        &fixture,
        &caller.pair.parent,
        &restart.work,
        caller.address,
        answer,
    )
    .await;
    stays(&fixture, &[called.job], "IN_PROGRESS").await;
    caller.pair.parent.complete_run(&restart.trigger).await;

    becomes_completed(&fixture, &[called.job]).await;
    one_completion_each(&durable, &[called.job]).await;
    durable.close().await;
    fixture.shutdown().await;
}
