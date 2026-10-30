mod support;

use std::borrow::Cow;

use br_test_harness::E2eDatabase;
use chrono::{DateTime, TimeDelta, Utc};
use sqlx::PgPool;
use sqlx::migrate::Migrator;
use support::fixture::{APP_PASSWORD, APP_ROLE, require_provisioned_infrastructure};
use uuid::Uuid;

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

const BEFORE_THE_EXPIRY_COLUMN: usize = 8;

async fn insert_result(
    pool: &PgPool,
    job: Uuid,
    run: Uuid,
    kept_at: DateTime<Utc>,
    read_at: Option<DateTime<Utc>>,
) -> Uuid {
    let result = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO job_results \
            (result_id, job_id, produced_by_run_id, addressed_run_id, message, attachments, \
             kept_at, read_at) \
         VALUES ($1, $2, $3, $3, 'answer', '[]'::jsonb, $4, $5)",
    )
    .bind(result)
    .bind(job)
    .bind(run)
    .bind(kept_at)
    .bind(read_at)
    .execute(pool)
    .await
    .expect("a Result inserts before the migration");
    result
}

async fn expires_at(pool: &PgPool, result: Uuid) -> DateTime<Utc> {
    sqlx::query_scalar("SELECT expires_at FROM job_results WHERE result_id = $1")
        .bind(result)
        .fetch_one(pool)
        .await
        .expect("the expiry reads")
}

#[tokio::test]
async fn results_kept_before_the_migration_expire_seven_days_after_their_last_read_or_their_keeping()
 {
    require_provisioned_infrastructure();
    let db = E2eDatabase::create(true, &[])
        .await
        .with_app_role(APP_ROLE, APP_PASSWORD)
        .await;
    let pool = PgPool::connect(&db.owner_migration_url())
        .await
        .expect("the owner connection opens");
    apply(&pool, BEFORE_THE_EXPIRY_COLUMN).await;
    let (job, run) = seed_run(
        &pool,
        &support::wire::unique_runner_type("result-expiry-migration"),
    )
    .await;
    let kept = Utc::now() - TimeDelta::days(5);
    let read = Utc::now() - TimeDelta::days(2);
    let unread = insert_result(&pool, job, run, kept, None).await;
    let read_once = insert_result(&pool, job, run, kept, Some(read)).await;

    apply(&pool, MIGRATOR.iter().count()).await;

    assert_eq!(expires_at(&pool, unread).await, kept + TimeDelta::days(7));
    assert_eq!(
        expires_at(&pool, read_once).await,
        read + TimeDelta::days(7)
    );
    let indexed: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_indexes WHERE indexname = 'job_results_expires'",
    )
    .fetch_one(&pool)
    .await
    .expect("the indexes read");
    assert_eq!(
        indexed, 1,
        "the purge finds expired Results by their expiry"
    );
    let missing = sqlx::query(
        "INSERT INTO job_results \
            (result_id, job_id, produced_by_run_id, addressed_run_id, message, attachments, kept_at) \
         VALUES ($1, $2, $3, $3, 'answer', '[]'::jsonb, $4)",
    )
    .bind(Uuid::now_v7())
    .bind(job)
    .bind(run)
    .bind(Utc::now())
    .execute(&pool)
    .await;
    assert!(missing.is_err(), "every Result carries its expiry");
    pool.close().await;
}

async fn apply(pool: &PgPool, count: usize) {
    let migrations = MIGRATOR.iter().take(count).cloned().collect::<Vec<_>>();
    assert_eq!(migrations.len(), count);
    Migrator {
        migrations: Cow::Owned(migrations),
        ignore_missing: true,
        locking: MIGRATOR.locking,
        no_tx: MIGRATOR.no_tx,
    }
    .run(pool)
    .await
    .expect("the schema applies through the real migrator");
}

async fn seed_run(pool: &PgPool, type_key: &str) -> (Uuid, Uuid) {
    let runner_type_id = Uuid::now_v7();
    let producer_id = Uuid::now_v7();
    let (job, run) = (Uuid::now_v7(), Uuid::now_v7());
    sqlx::query("INSERT INTO runner_types (id, type_key) VALUES ($1, $2)")
        .bind(runner_type_id)
        .bind(type_key)
        .execute(pool)
        .await
        .expect("the runner type inserts");
    sqlx::query("INSERT INTO producers (id, bc_key) VALUES ($1, 'projects')")
        .bind(producer_id)
        .execute(pool)
        .await
        .expect("the producer inserts");
    sqlx::query(
        "INSERT INTO jobs (id, runner_type_id, producer_id, created_at) VALUES ($1, $2, $3, $4)",
    )
    .bind(job)
    .bind(runner_type_id)
    .bind(producer_id)
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
