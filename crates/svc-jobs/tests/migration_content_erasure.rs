mod support;

use std::borrow::Cow;

use br_test_harness::E2eDatabase;
use chrono::{DateTime, Duration, Utc};
use sqlx::PgPool;
use sqlx::migrate::Migrator;
use support::fixture::{APP_PASSWORD, APP_ROLE, require_provisioned_infrastructure};
use uuid::Uuid;

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

const BEFORE_THE_ERASURE: usize = 11;
const THROUGH_THE_ERASURE: usize = 12;

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

async fn seed_job(pool: &PgPool, runner_type_id: Uuid, producer_id: Uuid) -> Uuid {
    let job = Uuid::now_v7();
    sqlx::query(
        "INSERT INTO jobs (id, runner_type_id, producer_id, created_at, passport, payload) \
         VALUES ($1, $2, $3, $4, '{\"kept\":true}'::jsonb, '{\"kept\":true}'::jsonb)",
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

async fn resolve(pool: &PgPool, job: Uuid, kind: &str, at: DateTime<Utc>) {
    sqlx::query(
        "INSERT INTO job_resolutions (id, job_id, kind, occurred_at) \
         VALUES ($1, $2, $3::job_resolution_kind, $4)",
    )
    .bind(Uuid::now_v7())
    .bind(job)
    .bind(kind)
    .bind(at)
    .execute(pool)
    .await
    .expect("the resolution inserts");
}

#[tokio::test]
async fn jobs_cancelled_before_the_erasure_wait_for_it_and_nothing_else_does() {
    require_provisioned_infrastructure();
    let db = E2eDatabase::create(true, &[])
        .await
        .with_app_role(APP_ROLE, APP_PASSWORD)
        .await;
    let pool = PgPool::connect(&db.owner_migration_url())
        .await
        .expect("the owner connection opens");
    apply(&pool, BEFORE_THE_ERASURE).await;
    let (runner_type_id, producer_id) = (Uuid::now_v7(), Uuid::now_v7());
    sqlx::query("INSERT INTO runner_types (id, type_key) VALUES ($1, $2)")
        .bind(runner_type_id)
        .bind(support::wire::unique_runner_type("content-erasure"))
        .execute(&pool)
        .await
        .expect("the runner type inserts");
    sqlx::query("INSERT INTO producers (id, bc_key) VALUES ($1, 'projects')")
        .bind(producer_id)
        .execute(&pool)
        .await
        .expect("the producer inserts");
    let cancelled_at = Utc::now() - Duration::days(9);
    let (cancelled, completed, _live) = (
        seed_job(&pool, runner_type_id, producer_id).await,
        seed_job(&pool, runner_type_id, producer_id).await,
        seed_job(&pool, runner_type_id, producer_id).await,
    );
    resolve(&pool, cancelled, "CANCELLED", cancelled_at).await;
    resolve(&pool, completed, "COMPLETED", Utc::now()).await;
    assert!(
        sqlx::query("SELECT content_erased_at FROM jobs")
            .fetch_all(&pool)
            .await
            .is_err(),
        "the first erasure time does not exist before the migration"
    );

    apply(&pool, THROUGH_THE_ERASURE).await;

    let erased: Vec<Option<DateTime<Utc>>> =
        sqlx::query_scalar("SELECT content_erased_at FROM jobs")
            .fetch_all(&pool)
            .await
            .expect("the first erasure time reads");
    assert_eq!(erased.len(), 3);
    assert!(
        erased.iter().all(Option::is_none),
        "no Job claims an erasure that did not happen"
    );
    let kept: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM jobs WHERE passport IS NOT NULL AND payload IS NOT NULL",
    )
    .fetch_one(&pool)
    .await
    .expect("the request data reads");
    assert_eq!(kept, 3, "the migration erases nothing by itself");
    let waiting: Vec<Uuid> = sqlx::query_scalar("SELECT job_id::uuid FROM cancelled_request_data")
        .fetch_all(&pool)
        .await
        .expect("the waiting Jobs read");
    assert_eq!(
        waiting,
        vec![cancelled],
        "a Job cancelled before waits; a completed or live one never"
    );
    let mails: Vec<Option<DateTime<Utc>>> =
        sqlx::query_scalar("SELECT content_erased_at FROM mails")
            .fetch_all(&pool)
            .await
            .expect("the mail erasure time exists");
    assert!(mails.is_empty());
    pool.close().await;
}
