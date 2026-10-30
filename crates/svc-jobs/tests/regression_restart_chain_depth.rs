mod support;

use bc_jobs::domain::ids::RunId;
use chrono::Utc;
use sqlx::PgPool;
use support::contention::Fixture;
use svc_jobs::db::acts_for::ACTS_FOR_SQL;
use svc_jobs::db::kept_payloads::PAYLOAD_SOURCE_SQL;
use svc_jobs::db::saved_states::saved_state_run_in;
use uuid::Uuid;

const RESTARTS: usize = 70;

async fn insert_job(pool: &PgPool, runner_type: Uuid, producer: Uuid) -> (Uuid, Uuid) {
    let (job, run) = (Uuid::now_v7(), Uuid::now_v7());
    sqlx::query(
        "INSERT INTO jobs (id, runner_type_id, producer_id, created_at) VALUES ($1, $2, $3, $4)",
    )
    .bind(job)
    .bind(runner_type)
    .bind(producer)
    .bind(Utc::now())
    .execute(pool)
    .await
    .expect("the job inserts");
    sqlx::query(
        "INSERT INTO runs (id, job_id, attempt_number, dispatched_at) VALUES ($1, $2, 1, $3)",
    )
    .bind(run)
    .bind(job)
    .bind(Utc::now())
    .execute(pool)
    .await
    .expect("the run inserts");
    (job, run)
}

#[tokio::test]
async fn a_long_restart_chain_still_reaches_the_first_payload_and_the_first_saved_state() {
    let fixture = Fixture::start().await;
    let pool = &fixture.pool;
    let (runner_type, producer) = (Uuid::now_v7(), Uuid::now_v7());
    sqlx::query("INSERT INTO runner_types (id, type_key) VALUES ($1, 'long-chain')")
        .bind(runner_type)
        .execute(pool)
        .await
        .expect("the runner type inserts");
    sqlx::query("INSERT INTO producers (id, bc_key) VALUES ($1, 'projects')")
        .bind(producer)
        .execute(pool)
        .await
        .expect("the producer inserts");
    let (first_job, first_run) = insert_job(pool, runner_type, producer).await;
    let mut previous_run = first_run;
    let mut last = (first_job, first_run);
    for _ in 0..RESTARTS {
        last = insert_job(pool, runner_type, producer).await;
        sqlx::query("INSERT INTO restart_origins (job_id, restarts_run_id) VALUES ($1, $2)")
            .bind(last.0)
            .bind(previous_run)
            .execute(pool)
            .await
            .expect("the restart link inserts");
        previous_run = last.1;
    }
    sqlx::query(
        "INSERT INTO kept_payloads (job_id, uploader, message, attachments, uploaded_at) \
         VALUES ($1, '{}'::jsonb, 'kept', '[]'::jsonb, $2)",
    )
    .bind(first_job)
    .bind(Utc::now())
    .execute(pool)
    .await
    .expect("the payload is kept");
    sqlx::query("INSERT INTO saved_states (run_id, state, saved_at) VALUES ($1, '{}'::jsonb, $2)")
        .bind(first_run)
        .bind(Utc::now())
        .execute(pool)
        .await
        .expect("the state is saved");

    let payload_source: Option<Uuid> = sqlx::query_scalar(PAYLOAD_SOURCE_SQL)
        .bind(last.0)
        .fetch_optional(pool)
        .await
        .expect("the payload source reads");
    assert_eq!(payload_source, Some(first_job));

    let saved = {
        let mut connection = pool.acquire().await.expect("a connection");
        saved_state_run_in(&mut connection, RunId::new(last.1).unwrap())
            .await
            .expect("the saved state reads")
    };
    assert_eq!(saved.map(|run| run.as_uuid()), Some(first_run));

    let acts_for: Vec<Uuid> = sqlx::query_scalar(ACTS_FOR_SQL)
        .bind(last.1)
        .fetch_all(pool)
        .await
        .expect("the acts-for chain reads");
    assert_eq!(acts_for.len(), RESTARTS);
    assert!(acts_for.contains(&first_run));
    fixture.shutdown().await;
}
