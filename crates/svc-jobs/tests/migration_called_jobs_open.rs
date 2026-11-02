mod support;

use std::borrow::Cow;

use br_test_harness::E2eDatabase;
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use sqlx::migrate::Migrator;
use support::fixture::{APP_PASSWORD, APP_ROLE, require_provisioned_infrastructure};
use uuid::Uuid;

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

const BEFORE: usize = 15;
const THROUGH: usize = 16;

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

async fn called_job(pool: &PgPool, runner_type_id: Uuid, producer_id: Uuid, called: bool) -> Uuid {
    let job = Uuid::now_v7();
    let (requester, call) = if called {
        (
            Some(serde_json::json!({ "Run": Uuid::now_v7().to_string() })),
            Some(serde_json::json!({ "caller_address": Uuid::now_v7().to_string() })),
        )
    } else {
        (None, None)
    };
    sqlx::query(
        "INSERT INTO jobs (id, runner_type_id, producer_id, created_at, requester, call) \
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(job)
    .bind(runner_type_id)
    .bind(producer_id)
    .bind(Utc::now())
    .bind(requester)
    .bind(call)
    .execute(pool)
    .await
    .expect("the job inserts");
    job
}

async fn resolve(pool: &PgPool, job: Uuid, at: DateTime<Utc>) {
    sqlx::query(
        "INSERT INTO job_resolutions (id, job_id, kind, occurred_at) \
         VALUES ($1, $2, 'COMPLETED', $3)",
    )
    .bind(Uuid::now_v7())
    .bind(job)
    .bind(at)
    .execute(pool)
    .await
    .expect("the resolution inserts");
}

async fn settled_at(pool: &PgPool, job: Uuid) -> Option<DateTime<Utc>> {
    sqlx::query_scalar("SELECT settled_at FROM jobs WHERE id = $1")
        .bind(job)
        .fetch_one(pool)
        .await
        .expect("the settled time reads")
}

#[tokio::test]
async fn the_called_jobs_already_resolved_are_settled_and_the_open_ones_stay_in_the_index() {
    require_provisioned_infrastructure();
    let db = E2eDatabase::create(true, &[])
        .await
        .with_app_role(APP_ROLE, APP_PASSWORD)
        .await;
    let pool = PgPool::connect(&db.owner_migration_url())
        .await
        .expect("the owner connection opens");
    apply(&pool, BEFORE).await;
    let (runner_type_id, producer_id) = (Uuid::now_v7(), Uuid::now_v7());
    sqlx::query("INSERT INTO runner_types (id, type_key) VALUES ($1, $2)")
        .bind(runner_type_id)
        .bind(support::wire::unique_runner_type("called-open"))
        .execute(&pool)
        .await
        .expect("the runner type inserts");
    sqlx::query("INSERT INTO producers (id, bc_key) VALUES ($1, 'projects')")
        .bind(producer_id)
        .execute(&pool)
        .await
        .expect("the producer inserts");
    let resolved_at = Utc::now() - chrono::TimeDelta::days(3);
    let resolved_called = called_job(&pool, runner_type_id, producer_id, true).await;
    resolve(&pool, resolved_called, resolved_at).await;
    let open_called = called_job(&pool, runner_type_id, producer_id, true).await;
    let open_plain = called_job(&pool, runner_type_id, producer_id, false).await;

    apply(&pool, THROUGH).await;

    assert_eq!(
        settled_at(&pool, resolved_called).await,
        Some(resolved_at),
        "a resolved called Job is settled at its resolution"
    );
    assert_eq!(settled_at(&pool, open_called).await, None);
    assert_eq!(settled_at(&pool, open_plain).await, None);
    let in_the_index: Vec<Uuid> = sqlx::query_scalar(
        "SELECT id::uuid FROM jobs \
         WHERE requester ? 'Run' AND call IS NOT NULL AND settled_at IS NULL",
    )
    .fetch_all(&pool)
    .await
    .expect("the open called Jobs read");
    assert_eq!(in_the_index, vec![open_called]);
    let definition: String =
        sqlx::query_scalar("SELECT indexdef FROM pg_indexes WHERE indexname = 'jobs_called_open'")
            .fetch_one(&pool)
            .await
            .expect("the index of the open called Jobs reads");
    assert!(
        definition.contains("settled_at IS NULL"),
        "the index holds only the called Jobs not yet settled: {definition}"
    );
    pool.close().await;
}
