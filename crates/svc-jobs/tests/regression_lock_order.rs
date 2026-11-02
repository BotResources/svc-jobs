mod support;

use std::time::Duration;

use bc_jobs::commands::job::cancellation::CancelJob;
use bc_jobs::domain::ids::ResolutionId;
use bc_jobs::domain::ownership::CancelRequester;
use sqlx::Connection;
use svc_jobs::app::write::JobChange;
use svc_jobs::db::kept_payloads::lock_kept_payload;
use uuid::Uuid;

use support::contention::{Fixture, a_queued_job, commit};

const RUNNER_TYPE: &str = "archivist";

async fn a_lock_wait_exists(fixture: &Fixture) -> bool {
    sqlx::query_scalar(
        "SELECT count(*) > 0 FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid \
         WHERE l.locktype = 'advisory' AND NOT l.granted AND a.datname = current_database()",
    )
    .fetch_one(&fixture.pool)
    .await
    .expect("the lock-wait probe runs")
}

async fn the_directory_lock_is_free(fixture: &Fixture, tree: Uuid) -> bool {
    let mut probe = fixture.pool.acquire().await.expect("a connection");
    let mut tx = probe.begin().await.expect("a transaction");
    let free: bool =
        sqlx::query_scalar("SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(format!("directory:{tree}"))
            .fetch_one(&mut *tx)
            .await
            .expect("the try-lock runs");
    tx.rollback().await.expect("the probe ends");
    free
}

#[tokio::test]
async fn a_cancel_waits_for_the_kept_payload_before_it_takes_the_directory_lock_of_the_tree() {
    let fixture = Fixture::start().await;
    let job = a_queued_job(&fixture, RUNNER_TYPE).await;
    let tree = job.initial_job_id().as_uuid();
    let mut creator = fixture.pool.begin().await.expect("the creator transaction");
    lock_kept_payload(&mut creator, job.id().as_uuid())
        .await
        .expect("the creator holds the kept payload");
    let cancelled = job
        .cancel(CancelJob {
            resolution_id: ResolutionId::new(Uuid::now_v7()).expect("a v7 resolution id"),
            requester: CancelRequester::Declarant,
        })
        .expect("a queued job cancels");

    let cancel = commit(
        &fixture,
        vec![JobChange::new(
            job.id(),
            Some(job.clone()),
            cancelled.events,
        )],
    );
    let observe = async {
        let mut probes = 0;
        while !a_lock_wait_exists(&fixture).await {
            probes += 1;
            assert!(probes < 200, "the cancel never waited for the kept payload");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let free = the_directory_lock_is_free(&fixture, tree).await;
        creator.commit().await.expect("the creator ends");
        free
    };
    let (outcome, tree_was_free) = tokio::join!(cancel, observe);

    outcome.expect("the cancel lands once the creator ends");
    assert!(
        tree_was_free,
        "the cancel waited for the kept payload while it held the directory lock of the Tree: a creator holding the kept payload and waiting for the Tree would deadlock with it"
    );
    fixture.shutdown().await;
}
