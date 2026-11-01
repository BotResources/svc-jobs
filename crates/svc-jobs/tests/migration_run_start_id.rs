mod support;

use std::borrow::Cow;

use br_test_harness::E2eDatabase;
use chrono::Utc;
use sqlx::PgPool;
use sqlx::migrate::Migrator;
use support::fixture::{APP_PASSWORD, APP_ROLE, require_provisioned_infrastructure};
use uuid::Uuid;

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

const BEFORE_THE_START_ID: usize = 9;

async fn instance_of(pool: &PgPool, run: Uuid) -> Uuid {
    let instance = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO runner_instances (id, runner_type_id, instance_key) \
         SELECT $1, j.runner_type_id, 'pod-1' FROM runs r JOIN jobs j ON j.id = r.job_id \
         WHERE r.id = $2",
    )
    .bind(instance)
    .bind(run)
    .execute(pool)
    .await
    .expect("the instance inserts");
    instance
}

async fn start(pool: &PgPool, run: Uuid, instance: Uuid, start_id: Option<Uuid>) -> bool {
    let sql = match start_id {
        Some(_) => {
            "INSERT INTO run_starts (run_id, instance_id, started_at, start_id) \
             VALUES ($1, $2, $3, $4)"
        }
        None => "INSERT INTO run_starts (run_id, instance_id, started_at) VALUES ($1, $2, $3)",
    };
    let query = sqlx::query(sql).bind(run).bind(instance).bind(Utc::now());
    let query = match start_id {
        Some(id) => query.bind(id),
        None => query,
    };
    query.execute(pool).await.is_ok()
}

async fn stored_start_id(pool: &PgPool, run: Uuid) -> Option<Uuid> {
    sqlx::query_scalar("SELECT start_id::uuid FROM run_starts WHERE run_id = $1")
        .bind(run)
        .fetch_one(pool)
        .await
        .expect("the start reads")
}

#[tokio::test]
async fn starts_held_before_the_migration_have_no_start_id_and_new_starts_may_carry_one() {
    require_provisioned_infrastructure();
    let db = E2eDatabase::create(true, &[])
        .await
        .with_app_role(APP_ROLE, APP_PASSWORD)
        .await;
    let pool = PgPool::connect(&db.owner_migration_url())
        .await
        .expect("the owner connection opens");
    apply(&pool, BEFORE_THE_START_ID).await;
    let type_key = support::wire::unique_runner_type("start-id-migration");
    let (_, held_before) = seed_run(&pool, &type_key).await;
    let instance = instance_of(&pool, held_before).await;
    assert!(start(&pool, held_before, instance, None).await);

    apply(&pool, MIGRATOR.iter().count()).await;

    assert_eq!(stored_start_id(&pool, held_before).await, None);
    let (_, held_after) = seed_run_of(&pool, &type_key).await;
    let id = Uuid::now_v7();
    assert!(start(&pool, held_after, instance, Some(id)).await);
    assert_eq!(stored_start_id(&pool, held_after).await, Some(id));
    let (_, refused) = seed_run_of(&pool, &type_key).await;
    assert!(
        !start(&pool, refused, instance, Some(Uuid::nil())).await,
        "a start id is a UUIDv7"
    );
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

async fn seed_run_of(pool: &PgPool, type_key: &str) -> (Uuid, Uuid) {
    let runner_type_id: Uuid =
        sqlx::query_scalar("SELECT id::uuid FROM runner_types WHERE type_key = $1")
            .bind(type_key)
            .fetch_one(pool)
            .await
            .expect("the runner type reads");
    let producer_id: Uuid = sqlx::query_scalar("SELECT id::uuid FROM producers LIMIT 1")
        .fetch_one(pool)
        .await
        .expect("the producer reads");
    insert_job_and_run(pool, runner_type_id, producer_id).await
}

async fn seed_run(pool: &PgPool, type_key: &str) -> (Uuid, Uuid) {
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
    insert_job_and_run(pool, runner_type_id, producer_id).await
}

async fn insert_job_and_run(
    pool: &PgPool,
    runner_type_id: Uuid,
    producer_id: Uuid,
) -> (Uuid, Uuid) {
    let (job, run) = (Uuid::now_v7(), Uuid::now_v7());
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
