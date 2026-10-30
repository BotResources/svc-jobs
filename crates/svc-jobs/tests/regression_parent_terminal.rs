mod support;

use chrono::Utc;
use sqlx::PgPool;
use support::contention::Fixture;
use svc_jobs::db::directory::PARENT_TERMINAL;
use uuid::Uuid;

async fn insert_job(
    pool: &PgPool,
    runner_type: Uuid,
    producer: Uuid,
    parent: Option<Uuid>,
) -> Uuid {
    let id = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO jobs (id, runner_type_id, producer_id, parent_job_id, created_at) \
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(id)
    .bind(runner_type)
    .bind(producer)
    .bind(parent)
    .bind(Utc::now())
    .execute(pool)
    .await
    .expect("the job inserts");
    id
}

async fn parent_terminal(pool: &PgPool, job: Uuid) -> bool {
    sqlx::query_scalar(&format!(
        "SELECT {PARENT_TERMINAL} FROM jobs j WHERE j.id = $1"
    ))
    .bind(job)
    .fetch_one(pool)
    .await
    .expect("the standing of the parent reads")
}

#[tokio::test]
async fn only_a_job_with_a_terminal_parent_has_a_terminal_parent() {
    let fixture = Fixture::start().await;
    let pool = &fixture.pool;
    let (runner_type, producer) = (Uuid::now_v7(), Uuid::now_v7());
    sqlx::query("INSERT INTO runner_types (id, type_key) VALUES ($1, 'parent-terminal')")
        .bind(runner_type)
        .execute(pool)
        .await
        .expect("the runner type inserts");
    sqlx::query("INSERT INTO producers (id, bc_key) VALUES ($1, 'projects')")
        .bind(producer)
        .execute(pool)
        .await
        .expect("the producer inserts");
    let root = insert_job(pool, runner_type, producer, None).await;
    let child = insert_job(pool, runner_type, producer, Some(root)).await;

    assert!(!parent_terminal(pool, root).await);
    assert!(!parent_terminal(pool, child).await);

    sqlx::query(
        "INSERT INTO job_resolutions (id, job_id, kind, occurred_at) \
         VALUES ($1, $2, 'COMPLETED', $3)",
    )
    .bind(Uuid::now_v7())
    .bind(root)
    .bind(Utc::now())
    .execute(pool)
    .await
    .expect("the root resolves");

    assert!(
        !parent_terminal(pool, root).await,
        "a root Job is its own parent only in the Tree, never for this question"
    );
    assert!(parent_terminal(pool, child).await);
    fixture.shutdown().await;
}
