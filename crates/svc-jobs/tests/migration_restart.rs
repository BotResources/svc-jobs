mod support;

use std::borrow::Cow;

use br_test_harness::E2eDatabase;
use chrono::Utc;
use sqlx::PgPool;
use sqlx::migrate::Migrator;
use support::fixture::{APP_PASSWORD, APP_ROLE, require_provisioned_infrastructure};
use uuid::Uuid;

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

const BEFORE_THE_RESTART: usize = 7;

#[tokio::test]
async fn the_directory_and_the_restart_links_survive_the_migration_and_wakes_follow_their_run() {
    require_provisioned_infrastructure();
    let db = E2eDatabase::create(true, &[])
        .await
        .with_app_role(APP_ROLE, APP_PASSWORD)
        .await;
    let pool = PgPool::connect(&db.owner_migration_url())
        .await
        .expect("the owner connection opens");
    apply(&pool, BEFORE_THE_RESTART).await;
    let (origin_job, origin_run) = seed_run(
        &pool,
        &support::wire::unique_runner_type("restart-migration"),
    )
    .await;
    let agent = Uuid::now_v7();
    insert_agent(&pool, agent, origin_run, origin_job).await;
    let (restart_job, _) = seed_job_and_run(&pool, origin_job).await;
    sqlx::query("INSERT INTO restart_origins (job_id, restarts_run_id) VALUES ($1, $2)")
        .bind(restart_job)
        .bind(origin_run)
        .execute(&pool)
        .await
        .expect("a restart link inserts before the migration");

    apply(&pool, MIGRATOR.iter().count()).await;

    let (kept, restarted_from): (i64, Option<Uuid>) = sqlx::query_as(
        "SELECT count(*), max(restarted_from_run_id::text)::uuid FROM directory_agents WHERE agent_id = $1",
    )
    .bind(agent)
    .fetch_one(&pool)
    .await
    .expect("the agent reads");
    assert_eq!(kept, 1, "a registered agent is untouched by the migration");
    assert_eq!(
        restarted_from, None,
        "no agent was restarted before the migration"
    );
    let links: i64 =
        sqlx::query_scalar("SELECT count(*) FROM restart_origins WHERE restarts_run_id = $1")
            .bind(origin_run)
            .fetch_one(&pool)
            .await
            .expect("the links read");
    assert_eq!(links, 1, "a restart link survives the migration");
    let indexed: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_indexes WHERE indexname = 'restart_origins_run'",
    )
    .fetch_one(&pool)
    .await
    .expect("the indexes read");
    assert_eq!(indexed, 1, "restarts are found by the Run they restart");

    sqlx::query("INSERT INTO restart_wakes (run_id, requested_at) VALUES ($1, $2)")
        .bind(origin_run)
        .bind(Utc::now())
        .execute(&pool)
        .await
        .expect("a wake names a Run of the ledger");
    let twice = sqlx::query("INSERT INTO restart_wakes (run_id, requested_at) VALUES ($1, $2)")
        .bind(origin_run)
        .bind(Utc::now())
        .execute(&pool)
        .await;
    assert!(twice.is_err(), "a Run has at most one wake");
    let stray = sqlx::query("INSERT INTO restart_wakes (run_id, requested_at) VALUES ($1, $2)")
        .bind(Uuid::now_v7())
        .bind(Utc::now())
        .execute(&pool)
        .await;
    assert!(
        stray.is_err(),
        "a wake cannot name a Run the ledger does not hold"
    );
    let unknown_origin =
        sqlx::query("UPDATE directory_agents SET restarted_from_run_id = $2 WHERE agent_id = $1")
            .bind(agent)
            .bind(Uuid::now_v7())
            .execute(&pool)
            .await;
    assert!(
        unknown_origin.is_err(),
        "an agent cannot be restarted from a Run the ledger does not hold"
    );
    pool.close().await;
}

async fn seed_job_and_run(pool: &PgPool, like_job: Uuid) -> (Uuid, Uuid) {
    let (job, run) = (Uuid::now_v7(), Uuid::now_v7());
    sqlx::query(
        "INSERT INTO jobs (id, runner_type_id, producer_id, created_at) \
         SELECT $1, runner_type_id, producer_id, $3 FROM jobs WHERE id = $2",
    )
    .bind(job)
    .bind(like_job)
    .bind(Utc::now())
    .execute(pool)
    .await
    .expect("the restart job inserts");
    sqlx::query(
        "INSERT INTO runs (id, job_id, attempt_number, dispatched_at) VALUES ($1, $2, 1, $3)",
    )
    .bind(run)
    .bind(job)
    .bind(Utc::now())
    .execute(pool)
    .await
    .expect("the restart run inserts");
    (job, run)
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
