mod support;

use bc_jobs::ports::PortError;
use bc_jobs::ports::scrub::ScrubOutcome;
use svc_jobs::db::tree_lock::lock_tree;
use uuid::Uuid;

use support::contention::Fixture;
use support::scrub_race::{
    a_completed_job_with_an_ended_run, a_failed_job, an_agent_waiting_for_a_restart,
    register_the_runner_type, scrub, try_scrub,
};

const ITEMS_OF_RUN: &str = "SELECT count(*) AS count FROM inbox_items WHERE run_id::text = $1";

#[tokio::test]
async fn a_notice_left_for_a_run_of_a_scrubbed_tree_is_erased_and_the_notice_of_another_tree_stays()
{
    let fixture = Fixture::start().await;
    register_the_runner_type(&fixture).await;
    let (scrubbed, scrubbed_run) = a_completed_job_with_an_ended_run(&fixture).await;
    let (_, other_run) = a_completed_job_with_an_ended_run(&fixture).await;
    an_agent_waiting_for_a_restart(&fixture, scrubbed_run, scrubbed.initial_job_id()).await;
    sqlx::query(
        "INSERT INTO inbox_items (item_id, kind, run_id, notice, accepted_at) \
         VALUES ($1, 'NOTICE', $2, '{\"type\": \"REFUSED\"}'::jsonb, now())",
    )
    .bind(Uuid::now_v7())
    .bind(other_run.as_uuid())
    .execute(&fixture.pool)
    .await
    .expect("the notice of the other Tree inserts");
    let (scrubbed_run, other_run) = (
        scrubbed_run.as_uuid().to_string(),
        other_run.as_uuid().to_string(),
    );
    assert_eq!(fixture.count(ITEMS_OF_RUN, &scrubbed_run).await, 1);

    assert_eq!(
        scrub(&fixture, scrubbed.initial_job_id()).await,
        ScrubOutcome::Scrubbed { jobs: 1 }
    );

    assert_eq!(fixture.count(ITEMS_OF_RUN, &scrubbed_run).await, 0);
    assert_eq!(fixture.count(ITEMS_OF_RUN, &other_run).await, 1);
    fixture.shutdown().await;
}

#[tokio::test]
async fn the_scrub_of_a_tree_is_refused_while_another_write_holds_the_tree() {
    let fixture = Fixture::start().await;
    let failed = a_failed_job(&fixture).await;
    let mut writing = fixture.pool.begin().await.expect("a transaction opens");
    lock_tree(&mut writing, failed.initial_job_id())
        .await
        .expect("a write holds the Tree");

    let refused = try_scrub(&fixture, failed.initial_job_id()).await;

    assert!(
        matches!(refused, Err(PortError::TreeLockTimeout)),
        "got {refused:?}"
    );
    writing.rollback().await.expect("the write ends");
    assert_eq!(
        scrub(&fixture, failed.initial_job_id()).await,
        ScrubOutcome::Scrubbed { jobs: 1 }
    );
    fixture.shutdown().await;
}
