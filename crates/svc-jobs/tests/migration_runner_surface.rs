mod support;

use std::borrow::Cow;

use br_test_harness::E2eDatabase;
use chrono::Utc;
use sqlx::PgPool;
use sqlx::migrate::Migrator;
use support::fixture::{APP_PASSWORD, APP_ROLE, require_provisioned_infrastructure};
use support::wire;
use uuid::Uuid;

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

const BEFORE_THE_RUNNER_SURFACE: usize = 6;

#[tokio::test]
async fn the_directory_survives_the_migration_and_the_inbox_follows_its_agents() {
    require_provisioned_infrastructure();
    let db = E2eDatabase::create(true, &[])
        .await
        .with_app_role(APP_ROLE, APP_PASSWORD)
        .await;
    let pool = PgPool::connect(&db.owner_migration_url())
        .await
        .expect("the owner connection opens");
    apply(&pool, BEFORE_THE_RUNNER_SURFACE).await;
    let (job, run) = seed_run(&pool, &wire::unique_runner_type("surface-migration")).await;
    let agent = Uuid::now_v7();
    insert_agent(&pool, agent, run, job).await;

    apply(&pool, MIGRATOR.iter().count()).await;

    let kept: i64 = sqlx::query_scalar("SELECT count(*) FROM directory_agents WHERE agent_id = $1")
        .bind(agent)
        .fetch_one(&pool)
        .await
        .expect("the agent survives");
    assert_eq!(kept, 1, "a registered agent is untouched by the migration");

    let runner_made = Uuid::new_v4();
    insert_agent(&pool, runner_made, run, job).await;

    sqlx::query(
        "INSERT INTO inbox_items (item_id, kind, address, run_id, notice, accepted_at) \
         VALUES ($1, 'NOTICE', $2, $3, '{\"type\":\"STARTED\"}'::jsonb, $4)",
    )
    .bind(Uuid::now_v7())
    .bind(agent)
    .bind(run)
    .bind(Utc::now())
    .execute(&pool)
    .await
    .expect("a notice at an address of the directory inserts");
    let shapeless = sqlx::query(
        "INSERT INTO inbox_items (item_id, kind, address, accepted_at) \
         VALUES ($1, 'MAIL', $2, $3)",
    )
    .bind(Uuid::now_v7())
    .bind(agent)
    .bind(Utc::now())
    .execute(&pool)
    .await;
    assert!(shapeless.is_err(), "a mail item must name its mail");
    let stray = sqlx::query(
        "INSERT INTO inbox_items (item_id, kind, address, run_id, notice, accepted_at) \
         VALUES ($1, 'NOTICE', $2, $3, '{}'::jsonb, $4)",
    )
    .bind(Uuid::now_v7())
    .bind(Uuid::now_v7())
    .bind(run)
    .bind(Utc::now())
    .execute(&pool)
    .await;
    assert!(
        stray.is_err(),
        "an item cannot name an address the directory does not hold"
    );

    sqlx::query("DELETE FROM directory_agents WHERE agent_id = $1")
        .bind(agent)
        .execute(&pool)
        .await
        .expect("the agent leaves the directory");
    let left: i64 = sqlx::query_scalar("SELECT count(*) FROM inbox_items")
        .fetch_one(&pool)
        .await
        .expect("the inbox reads");
    assert_eq!(
        left, 0,
        "an address that leaves the directory takes its items with it"
    );
    pool.close().await;
}

async fn insert_agent(pool: &PgPool, agent: Uuid, run: Uuid, job: Uuid) {
    sqlx::query(
        "INSERT INTO directory_agents \
            (agent_id, run_id, initial_job_id, has_inbox, declares_restart, state, registered_at) \
         VALUES ($1, $2, $3, true, true, 'RUNNING', $4)",
    )
    .bind(agent)
    .bind(run)
    .bind(job)
    .bind(Utc::now())
    .execute(pool)
    .await
    .expect("the agent inserts");
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
