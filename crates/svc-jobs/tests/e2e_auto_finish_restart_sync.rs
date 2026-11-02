mod support;

use support::auto_finish::{
    becomes_completed, complete, mark_the_mail_as, mark_the_notice, one_completion_each, reply_to,
    restartable, started_restart, stays, unread_mails,
};
use support::call_notices::caller;
use support::db::Durable;
use support::fixture::JobsFixture;
use support::restart::RESTARTS_OF_RUN;
use uuid::Uuid;

#[tokio::test]
async fn a_sync_call_whose_runner_restarts_after_a_reply_is_finished_on_the_notice_the_caller_marked()
 {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let mut caller = caller(&fixture, "auto-sync-restart").await;
    let n = restartable(&fixture, &mut caller, false).await;
    complete(&caller, &n.called).await;
    mark_the_notice(&fixture, &caller, &n.called).await;

    reply_to(&fixture, &caller, n.agent).await;
    durable
        .await_count(
            RESTARTS_OF_RUN,
            n.called.work.run_id,
            1,
            "the reply restarts",
        )
        .await;
    let restart_job: Uuid =
        sqlx::query_scalar("SELECT job_id::uuid FROM restart_origins WHERE restarts_run_id = $1")
            .bind(n.called.work.run_id)
            .fetch_one(durable.pool())
            .await
            .expect("the restart job exists");
    caller.pair.parent.complete_run(&caller.trigger).await;
    stays(&fixture, &[n.called.job, restart_job], "IN_PROGRESS").await;

    let restart = started_restart(&fixture, &mut caller.pair.child, n.agent).await;
    for reply in unread_mails(&fixture, &restart.work, n.agent).await {
        let id = reply["mailId"].as_str().unwrap().parse().unwrap();
        mark_the_mail_as(&fixture, &caller.pair.child, &restart.work, n.agent, id).await;
    }
    caller.pair.child.complete_run(&restart.trigger).await;

    becomes_completed(&fixture, &[n.called.job, restart_job]).await;
    one_completion_each(&durable, &[n.called.job, restart_job]).await;
    durable.close().await;
    fixture.shutdown().await;
}
