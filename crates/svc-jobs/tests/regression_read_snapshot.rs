mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use chrono::Utc;
use sqlx::PgPool;
use svc_jobs::db::PgStore;
use uuid::Uuid;

use support::contention::{Fixture, a_queued_job};

const RUNNER_TYPE: &str = "archivist";
const JOBS: usize = 60;
const READERS: usize = 4;

async fn cancel_and_erase(pool: &PgPool, job: Uuid) {
    let mut tx = pool.begin().await.expect("the writer transaction opens");
    sqlx::query(
        "INSERT INTO job_resolutions (id, job_id, kind, occurred_at) \
         VALUES ($1, $2, 'CANCELLED'::job_resolution_kind, $3)",
    )
    .bind(Uuid::now_v7())
    .bind(job)
    .bind(Utc::now())
    .execute(&mut *tx)
    .await
    .expect("the resolution inserts");
    sqlx::query(
        "UPDATE jobs SET config = NULL, passport = NULL, payload = NULL, \
            content_erased_at = now() WHERE id = $1",
    )
    .bind(job)
    .execute(&mut *tx)
    .await
    .expect("the request data is erased");
    tx.commit().await.expect("the writer commits");
}

async fn read_until_done(store: PgStore, ids: Vec<Uuid>, done: Arc<AtomicBool>) -> usize {
    let mut reads = 0;
    while !done.load(Ordering::Acquire) {
        store
            .load_batch(&ids)
            .await
            .unwrap_or_else(|error| panic!("a read met a torn Job: {error:?}"));
        reads += 1;
    }
    reads
}

#[tokio::test]
async fn a_read_never_meets_a_job_erased_without_its_resolution_while_a_cancel_commits() {
    let fixture = Fixture::start().await;
    let mut ids = Vec::with_capacity(JOBS);
    for _ in 0..JOBS {
        ids.push(a_queued_job(&fixture, RUNNER_TYPE).await.id().as_uuid());
    }
    let done = Arc::new(AtomicBool::new(false));
    let readers: Vec<_> = (0..READERS)
        .map(|_| {
            tokio::spawn(read_until_done(
                fixture.store.clone(),
                ids.clone(),
                done.clone(),
            ))
        })
        .collect();

    for id in &ids {
        cancel_and_erase(&fixture.pool, *id).await;
        tokio::task::yield_now().await;
    }
    done.store(true, Ordering::Release);

    let mut reads = 0;
    for reader in readers {
        reads += reader.await.expect("the reader ends without a panic");
    }
    assert!(reads > 0, "the readers read while the writer committed");
    let settled = fixture
        .store
        .load_batch(&ids)
        .await
        .expect("every cancelled Job loads once the writer ends");
    assert!(settled.iter().all(|job| job.content_erased_at().is_some()));
    fixture.shutdown().await;
}
