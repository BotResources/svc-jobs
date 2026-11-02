mod support;

use std::borrow::Cow;

use br_test_harness::E2eDatabase;
use chrono::Utc;
use sqlx::PgPool;
use sqlx::migrate::Migrator;
use support::fixture::{APP_PASSWORD, APP_ROLE, require_provisioned_infrastructure};
use uuid::Uuid;

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

const BEFORE_THE_SCRUB: usize = 12;
const THROUGH_THE_SCRUB: usize = 13;

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
async fn the_scrub_marker_starts_empty_and_no_job_is_taken_for_scrubbed() {
    require_provisioned_infrastructure();
    let db = E2eDatabase::create(true, &[])
        .await
        .with_app_role(APP_ROLE, APP_PASSWORD)
        .await;
    let pool = PgPool::connect(&db.owner_migration_url())
        .await
        .expect("the owner connection opens");
    apply(&pool, BEFORE_THE_SCRUB).await;
    let (runner_type_id, producer_id, job) = (Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7());
    sqlx::query("INSERT INTO runner_types (id, type_key) VALUES ($1, $2)")
        .bind(runner_type_id)
        .bind(support::wire::unique_runner_type("tree-scrub"))
        .execute(&pool)
        .await
        .expect("the runner type inserts");
    sqlx::query("INSERT INTO producers (id, bc_key) VALUES ($1, 'projects')")
        .bind(producer_id)
        .execute(&pool)
        .await
        .expect("the producer inserts");
    sqlx::query(
        "INSERT INTO jobs (id, runner_type_id, producer_id, created_at, passport, payload) \
         VALUES ($1, $2, $3, $4, '{\"kept\":true}'::jsonb, '{\"kept\":true}'::jsonb)",
    )
    .bind(job)
    .bind(runner_type_id)
    .bind(producer_id)
    .bind(Utc::now())
    .execute(&pool)
    .await
    .expect("the job inserts");
    assert!(
        sqlx::query("SELECT count(*) FROM tree_scrubs")
            .fetch_one(&pool)
            .await
            .is_err(),
        "the scrub marker does not exist before the migration"
    );

    apply(&pool, THROUGH_THE_SCRUB).await;

    let scrubbed: i64 = sqlx::query_scalar("SELECT count(*) FROM tree_scrubs")
        .fetch_one(&pool)
        .await
        .expect("the scrub marker reads");
    assert_eq!(scrubbed, 0, "the migration scrubs no Tree by itself");
    let kept: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM jobs WHERE passport IS NOT NULL AND payload IS NOT NULL",
    )
    .fetch_one(&pool)
    .await
    .expect("the request data reads");
    assert_eq!(kept, 1, "the migration erases nothing by itself");
    sqlx::query("INSERT INTO tree_scrubs (initial_job_id, scrubbed_at) VALUES ($1, now())")
        .bind(job)
        .execute(&pool)
        .await
        .expect("a Tree is marked once");
    assert!(
        sqlx::query("INSERT INTO tree_scrubs (initial_job_id, scrubbed_at) VALUES ($1, now())")
            .bind(job)
            .execute(&pool)
            .await
            .is_err(),
        "a Tree is never marked twice"
    );
    let unmarked: i64 =
        sqlx::query_scalar("SELECT count(*) FROM jobs WHERE tree_scrubbed_at IS NOT NULL")
            .fetch_one(&pool)
            .await
            .expect("the access path of the scrubbed Trees reads");
    assert_eq!(unmarked, 0, "no Job is taken for a scrubbed Initial Job");
    let initial_index: String = sqlx::query_scalar(
        "SELECT indexdef FROM pg_indexes WHERE indexname = 'jobs_initial_created'",
    )
    .fetch_one(&pool)
    .await
    .expect("the index of the Initial Jobs reads");
    assert!(
        initial_index.contains("tree_scrubbed_at IS NULL"),
        "the index of the Initial Jobs holds only the Trees not yet scrubbed: {initial_index}"
    );
    let indexes: Vec<String> = sqlx::query_scalar(
        "SELECT indexname FROM pg_indexes WHERE indexname IN \
         ('integration_outbox_published_job', 'jobs_without_root', 'mails_initial_job') \
         ORDER BY indexname",
    )
    .fetch_all(&pool)
    .await
    .expect("the indexes read");
    assert_eq!(
        indexes,
        vec![
            "integration_outbox_published_job",
            "jobs_without_root",
            "mails_initial_job"
        ]
    );
    pool.close().await;
}
