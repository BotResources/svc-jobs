mod support;

use std::time::Duration;

use bc_jobs::ports::job::DUE_WORK_BATCH;
use br_test_harness::wait_until;
use sqlx::PgPool;
use support::auto_finish::{complete, sync_call};
use support::fixture::JobsFixture;
use support::gesture;
use support::runner;
use uuid::Uuid;

const COMPLETED_CALLS: &str = "SELECT count(*) FROM job_resolutions r \
     JOIN jobs j ON j.id = r.job_id \
     WHERE j.parent_job_id = $1 AND r.kind = 'COMPLETED'";

const NOTICES: &str = "SELECT count(*) FROM inbox_items \
     WHERE kind = 'NOTICE' AND run_id = $1 AND notice ->> 'type' = 'RUN_COMPLETED'";

const NOTICE_OF_JOB: &str = "SELECT item_id::uuid FROM inbox_items \
     WHERE kind = 'NOTICE' AND run_id = $1 AND (notice ->> 'job_id')::uuid = $2";

const SWEEP_WAIT: Duration = Duration::from_secs(60);

async fn count(pool: &PgPool, sql: &str, id: Uuid) -> i64 {
    sqlx::query_scalar(sql)
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("the rows count")
}

#[tokio::test]
async fn the_sweep_reaches_a_finishable_called_job_behind_a_full_page_of_jobs_it_cannot_finish() {
    let fixture = JobsFixture::start().await;
    let owner = PgPool::connect(&fixture.database().owner_migration_url())
        .await
        .expect("the owner connection opens");
    let mut caller = support::call_notices::caller(&fixture, "auto-finish-paging").await;
    let waiting = i64::try_from(DUE_WORK_BATCH + 5).expect("a count");

    for _ in 0..waiting {
        let called = sync_call(&fixture, &mut caller).await;
        complete(&caller, &called).await;
    }
    let finishable = sync_call(&fixture, &mut caller).await;
    complete(&caller, &finishable).await;
    let caller_run = runner::run_id(&caller.trigger);

    let noticed = wait_until(SWEEP_WAIT, || async {
        count(&owner, NOTICES, caller_run).await == waiting + 1
    })
    .await;
    assert!(
        noticed,
        "every called job tells its caller that its Run completed"
    );
    let item: Uuid = sqlx::query_scalar(NOTICE_OF_JOB)
        .bind(caller_run)
        .bind(finishable.job)
        .fetch_one(&owner)
        .await
        .expect("the notice of the last called job");
    let marked = gesture::mark_read(fixture.nats(), &caller.pair.parent, caller_run, &[item]).await;
    assert_eq!(marked["accepted"], true, "{marked}");
    caller.pair.parent.complete_run(&caller.trigger).await;

    let finished = wait_until(SWEEP_WAIT, || async {
        count(&owner, COMPLETED_CALLS, caller.root).await == 1
    })
    .await;
    assert!(
        finished,
        "the sweep never reached the last called job behind {waiting} jobs whose notice is unread"
    );
    assert_eq!(
        count(
            &owner,
            "SELECT count(*) FROM job_resolutions WHERE job_id = $1",
            finishable.job
        )
        .await,
        1,
        "the finishable job is the one that was finished"
    );
    owner.close().await;
    fixture.shutdown().await;
}
