mod support;

use std::borrow::Cow;

use br_test_harness::E2eDatabase;
use chrono::Utc;
use serde_json::json;
use sqlx::PgPool;
use sqlx::migrate::Migrator;
use support::fixture::{APP_PASSWORD, APP_ROLE, require_provisioned_infrastructure};
use support::wire;
use uuid::Uuid;

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

const BEFORE_THE_CAPACITY_LEDGER: usize = 4;
const BEFORE_THE_DIRECTORY: usize = 5;

#[tokio::test]
async fn a_job_requested_by_a_run_is_linked_to_that_run_and_the_directory_starts_empty() {
    require_provisioned_infrastructure();
    let db = E2eDatabase::create(true, &[])
        .await
        .with_app_role(APP_ROLE, APP_PASSWORD)
        .await;
    let pool = PgPool::connect(&db.owner_migration_url())
        .await
        .expect("the owner connection opens");
    apply(&pool, BEFORE_THE_DIRECTORY).await;
    let type_key = wire::unique_runner_type("directory-migration");
    let (parent, run, child, plain) = seed(&pool, &type_key).await;
    apply(&pool, MIGRATOR.iter().count()).await;

    let linked: Option<Uuid> = sqlx::query_scalar("SELECT parent_run_id FROM jobs WHERE id = $1")
        .bind(child)
        .fetch_one(&pool)
        .await
        .expect("the child survives the migration");
    assert_eq!(linked, Some(run), "a Run's call belongs to that Run");
    for unlinked in [parent, plain] {
        let found: Option<Uuid> =
            sqlx::query_scalar("SELECT parent_run_id FROM jobs WHERE id = $1")
                .bind(unlinked)
                .fetch_one(&pool)
                .await
                .expect("the job survives the migration");
        assert_eq!(found, None, "the migration invents no Run for a job");
    }
    let held: i64 = sqlx::query_scalar(
        "SELECT (SELECT count(*) FROM directory_agents) \
         + (SELECT count(*) FROM directory_removed_runs)",
    )
    .fetch_one(&pool)
    .await
    .expect("the directory tables exist");
    assert_eq!(held, 0);
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

async fn seed(pool: &PgPool, type_key: &str) -> (Uuid, Uuid, Uuid, Uuid) {
    let runner_type_id = Uuid::now_v7();
    let producer_id = Uuid::now_v7();
    let (parent, run, child, plain) = (
        Uuid::now_v7(),
        Uuid::now_v7(),
        Uuid::now_v7(),
        Uuid::now_v7(),
    );
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
    for (id, parent_job, requester) in [
        (parent, None, None),
        (plain, Some(parent), None),
        (child, Some(parent), Some(json!({ "Run": run.to_string() }))),
    ] {
        if id == child {
            sqlx::query(
                "INSERT INTO runs (id, job_id, attempt_number, dispatched_at) \
                 VALUES ($1, $2, 1, $3)",
            )
            .bind(run)
            .bind(parent)
            .bind(Utc::now())
            .execute(pool)
            .await
            .expect("the run of the parent inserts");
        }
        sqlx::query(
            "INSERT INTO jobs (id, runner_type_id, producer_id, parent_job_id, created_at, \
             requester) VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(id)
        .bind(runner_type_id)
        .bind(producer_id)
        .bind(parent_job)
        .bind(Utc::now())
        .bind(requester)
        .execute(pool)
        .await
        .expect("the job inserts");
    }
    (parent, run, child, plain)
}

#[tokio::test]
async fn the_capacity_ledger_is_backfilled_from_the_runs_that_have_not_ended() {
    require_provisioned_infrastructure();
    let db = E2eDatabase::create(true, &[])
        .await
        .with_app_role(APP_ROLE, APP_PASSWORD)
        .await;
    let pool = PgPool::connect(&db.owner_migration_url())
        .await
        .expect("the owner connection opens");
    apply(&pool, BEFORE_THE_CAPACITY_LEDGER).await;
    let type_key = wire::unique_runner_type("ledger-migration");
    let (live, ended) = seed_runs(&pool, &type_key).await;
    apply(&pool, BEFORE_THE_CAPACITY_LEDGER + 1).await;

    let held: Vec<Uuid> = sqlx::query_scalar("SELECT run_id::uuid FROM current_runs")
        .fetch_all(&pool)
        .await
        .expect("the ledger exists");
    assert_eq!(
        held,
        vec![live],
        "only a run that has not ended holds a slot"
    );
    assert!(!held.contains(&ended));
    pool.close().await;
}

async fn seed_runs(pool: &PgPool, type_key: &str) -> (Uuid, Uuid) {
    let runner_type_id = Uuid::now_v7();
    let producer_id = Uuid::now_v7();
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
    let (live, ended) = (Uuid::now_v7(), Uuid::now_v7());
    for run in [live, ended] {
        let job = Uuid::now_v7();
        sqlx::query(
            "INSERT INTO jobs (id, runner_type_id, producer_id, created_at) \
             VALUES ($1, $2, $3, $4)",
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
    }
    sqlx::query(
        "INSERT INTO run_terminals (run_id, kind, occurred_at) VALUES ($1, 'COMPLETED', $2)",
    )
    .bind(ended)
    .bind(Utc::now())
    .execute(pool)
    .await
    .expect("the run ends");
    (live, ended)
}
