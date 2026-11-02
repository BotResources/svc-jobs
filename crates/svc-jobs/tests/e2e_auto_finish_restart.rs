mod support;

use serde_json::json;
use support::auto_finish::{
    becomes_completed, complete, mark_the_mail, mark_the_mail_as, mark_the_notice,
    one_completion_each, reply_to, restartable, send_answer, send_answer_as, started_restart,
    stays, sync_call, unread_mails,
};
use support::call_notices::caller;
use support::db::Durable;
use support::events::EventLog;
use support::fixture::JobsFixture;
use support::lineage::act;
use support::restart::{RESTARTS_OF_RUN, save_state};
use support::{LONG, wire};
use uuid::Uuid;

const RESTART_JOB_OF_RUN: &str =
    "SELECT job_id::uuid FROM restart_origins WHERE restarts_run_id = $1";

async fn restart_job_of(durable: &Durable, run: Uuid) -> Uuid {
    sqlx::query_scalar(RESTART_JOB_OF_RUN)
        .bind(run)
        .fetch_one(durable.pool())
        .await
        .expect("the restart job exists")
}

#[tokio::test]
async fn a_reply_that_restarts_the_called_runner_holds_the_finish_until_the_restart_run_is_read() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let events = EventLog::open_v2(fixture.fabric()).await;
    let mut caller = caller(&fixture, "auto-restart").await;
    save_state(&fixture, &caller.work, json!({ "turn": 1 })).await;

    let swept = sync_call(&fixture, &mut caller).await;
    complete(&caller, &swept).await;
    mark_the_notice(&fixture, &caller, &swept).await;

    let m = restartable(&fixture, &mut caller, true).await;
    let first = send_answer(&fixture, &caller, &m.called, m.agent).await;
    complete(&caller, &m.called).await;
    mark_the_mail(&fixture, &caller, first).await;

    reply_to(&fixture, &caller, m.agent).await;
    durable
        .await_count(
            RESTARTS_OF_RUN,
            m.called.work.run_id,
            1,
            "the reply restarts the agent",
        )
        .await;
    reply_to(&fixture, &caller, m.agent).await;
    durable
        .assert_count(
            RESTARTS_OF_RUN,
            m.called.work.run_id,
            1,
            "a second reply while the restart job waits starts no second restart job",
        )
        .await;
    let restart_job = restart_job_of(&durable, m.called.work.run_id).await;

    caller.pair.parent.complete_run(&caller.trigger).await;
    becomes_completed(&fixture, &[swept.job]).await;
    stays(&fixture, &[m.called.job, restart_job], "IN_PROGRESS").await;

    let restart = started_restart(&fixture, &mut caller.pair.child, m.agent).await;
    let replies = unread_mails(&fixture, &restart.work, m.agent).await;
    assert_eq!(replies.len(), 2, "the same Run reads both replies");
    for reply in &replies {
        let id = reply["mailId"].as_str().unwrap().parse().unwrap();
        mark_the_mail_as(&fixture, &caller.pair.child, &restart.work, m.agent, id).await;
    }
    let second = send_answer_as(&fixture, &restart.work, m.agent, caller.address).await;
    caller.pair.child.complete_run(&restart.trigger).await;
    stays(&fixture, &[m.called.job, restart_job], "IN_PROGRESS").await;

    let reading = started_restart(&fixture, &mut caller.pair.parent, caller.address).await;
    mark_the_mail_as(
        &fixture,
        &caller.pair.parent,
        &reading.work,
        caller.address,
        second,
    )
    .await;
    act(&fixture, wire::VERB_FINISH, swept.job, &reading.work).await;
    let refused = events
        .expect_exactly(wire::FACT_COMMAND_REJECTED, swept.job, 1, LONG)
        .await;
    assert_eq!(
        refused[0].payload()["reason_code"],
        "job_terminal",
        "a late finish by the library is refused: the job is already finished"
    );
    stays(&fixture, &[m.called.job, restart_job], "IN_PROGRESS").await;

    caller.pair.parent.complete_run(&reading.trigger).await;
    becomes_completed(&fixture, &[m.called.job, restart_job]).await;
    one_completion_each(&durable, &[m.called.job, restart_job, swept.job]).await;
    for job in [m.called.job, restart_job, swept.job] {
        events
            .expect_exactly(wire::FACT_COMPLETED, job, 1, LONG)
            .await;
    }
    events.stop().await;
    durable.close().await;
    fixture.shutdown().await;
}
