mod support;

use std::borrow::Cow;

use br_test_harness::E2eDatabase;
use chrono::Utc;
use sqlx::PgPool;
use sqlx::migrate::Migrator;
use support::fixture::{APP_PASSWORD, APP_ROLE, require_provisioned_infrastructure};
use uuid::Uuid;

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

const BEFORE_THE_COMMAND_ID: usize = 12;

async fn seed_job(pool: &PgPool, runner_type_id: Uuid, producer_id: Uuid) -> Uuid {
    let job = Uuid::now_v7();
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
    job
}

async fn resolve(pool: &PgPool, job: Uuid, command_id: Option<Uuid>) -> bool {
    let sql = match command_id {
        Some(_) => {
            "INSERT INTO job_resolutions (id, job_id, kind, occurred_at, command_id) \
             VALUES ($1, $2, 'COMPLETED', $3, $4)"
        }
        None => {
            "INSERT INTO job_resolutions (id, job_id, kind, occurred_at) \
             VALUES ($1, $2, 'COMPLETED', $3)"
        }
    };
    let query = sqlx::query(sql)
        .bind(Uuid::now_v7())
        .bind(job)
        .bind(Utc::now());
    let query = match command_id {
        Some(id) => query.bind(id),
        None => query,
    };
    query.execute(pool).await.is_ok()
}

async fn stored_command(pool: &PgPool, job: Uuid) -> Option<Uuid> {
    sqlx::query_scalar("SELECT command_id FROM job_resolutions WHERE job_id = $1")
        .bind(job)
        .fetch_one(pool)
        .await
        .expect("the resolution reads")
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

#[tokio::test]
async fn resolutions_made_before_the_migration_remember_no_command_and_new_ones_may() {
    require_provisioned_infrastructure();
    let db = E2eDatabase::create(true, &[])
        .await
        .with_app_role(APP_ROLE, APP_PASSWORD)
        .await;
    let pool = PgPool::connect(&db.owner_migration_url())
        .await
        .expect("the owner connection opens");
    apply(&pool, BEFORE_THE_COMMAND_ID).await;
    let (runner_type_id, producer_id) = (Uuid::now_v7(), Uuid::now_v7());
    sqlx::query("INSERT INTO runner_types (id, type_key) VALUES ($1, 'command-memory')")
        .bind(runner_type_id)
        .execute(&pool)
        .await
        .expect("the runner type inserts");
    sqlx::query("INSERT INTO producers (id, bc_key) VALUES ($1, 'projects')")
        .bind(producer_id)
        .execute(&pool)
        .await
        .expect("the producer inserts");
    let before = seed_job(&pool, runner_type_id, producer_id).await;
    assert!(resolve(&pool, before, None).await);

    apply(&pool, MIGRATOR.iter().count()).await;

    assert_eq!(stored_command(&pool, before).await, None);
    let after = seed_job(&pool, runner_type_id, producer_id).await;
    let command = Uuid::now_v7();
    assert!(resolve(&pool, after, Some(command)).await);
    assert_eq!(stored_command(&pool, after).await, Some(command));
    assert!(
        !resolve(&pool, after, Some(command)).await,
        "one command resolves a Job once"
    );
    let plain = seed_job(&pool, runner_type_id, producer_id).await;
    assert!(
        resolve(&pool, plain, Some(command)).await,
        "the same command id on another Job is another resolution"
    );
    pool.close().await;
}
