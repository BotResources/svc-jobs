mod support;

use std::borrow::Cow;

use br_test_harness::E2eDatabase;
use chrono::Utc;
use sqlx::PgPool;
use sqlx::migrate::Migrator;
use support::fixture::{APP_PASSWORD, APP_ROLE, require_provisioned_infrastructure};
use uuid::Uuid;

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

const BEFORE_THE_MAIL_NOTICE: usize = 10;
const THROUGH_THE_MAIL_NOTICE: usize = 11;

async fn agent_of(pool: &PgPool, run: Uuid, job: Uuid) -> Uuid {
    let agent = Uuid::now_v7();
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
    agent
}

async fn notice_item(
    pool: &PgPool,
    kind: &str,
    address: Option<Uuid>,
    run: Option<Uuid>,
    source_key: Option<&str>,
) -> Result<(), sqlx::Error> {
    let sql = match source_key {
        Some(_) => {
            "INSERT INTO inbox_items \
                (item_id, kind, address, run_id, notice, accepted_at, source_key) \
             VALUES ($1, $2, $3, $4, '{\"type\":\"REFUSED\"}'::jsonb, $5, $6)"
        }
        None => {
            "INSERT INTO inbox_items (item_id, kind, address, run_id, notice, accepted_at) \
             VALUES ($1, $2, $3, $4, '{\"type\":\"REFUSED\"}'::jsonb, $5)"
        }
    };
    let query = sqlx::query(sql)
        .bind(Uuid::now_v7())
        .bind(kind)
        .bind(address)
        .bind(run)
        .bind(Utc::now());
    let query = match source_key {
        Some(key) => query.bind(key),
        None => query,
    };
    query.execute(pool).await.map(|_| ())
}

#[tokio::test]
async fn items_kept_before_the_mail_notice_stay_and_a_mail_notice_has_its_own_shape() {
    require_provisioned_infrastructure();
    let db = E2eDatabase::create(true, &[])
        .await
        .with_app_role(APP_ROLE, APP_PASSWORD)
        .await;
    let pool = PgPool::connect(&db.owner_migration_url())
        .await
        .expect("the owner connection opens");
    apply(&pool, BEFORE_THE_MAIL_NOTICE).await;
    let (job, run) = seed_run(&pool, &support::wire::unique_runner_type("mail-notice")).await;
    let agent = agent_of(&pool, run, job).await;
    notice_item(&pool, "NOTICE", Some(agent), Some(run), None)
        .await
        .expect("a call notice inserts before the migration");
    assert!(
        notice_item(&pool, "MAIL_NOTICE", Some(agent), Some(run), None)
            .await
            .is_err(),
        "the fourth kind does not exist before the migration"
    );

    apply(&pool, THROUGH_THE_MAIL_NOTICE).await;

    let kept: i64 = sqlx::query_scalar("SELECT count(*) FROM inbox_items WHERE kind = 'NOTICE'")
        .fetch_one(&pool)
        .await
        .expect("the inbox reads");
    assert_eq!(kept, 1, "an item kept before the migration is untouched");
    let no_key: Option<String> = sqlx::query_scalar("SELECT source_key FROM inbox_items")
        .fetch_one(&pool)
        .await
        .expect("the key reads");
    assert_eq!(no_key, None, "an old item has no source key");
    notice_item(&pool, "MAIL_NOTICE", Some(agent), Some(run), None)
        .await
        .expect("a mail notice has an address, a Run and a notice");
    assert!(
        notice_item(&pool, "MAIL_NOTICE", None, Some(run), None)
            .await
            .is_err(),
        "a mail notice has an address"
    );
    assert!(
        notice_item(&pool, "MAIL_NOTICE", Some(agent), None, None)
            .await
            .is_err(),
        "a mail notice has a Run"
    );
    let with_mail = sqlx::query(
        "INSERT INTO inbox_items (item_id, kind, address, run_id, mail_id, notice, accepted_at) \
         VALUES ($1, 'MAIL_NOTICE', $2, $3, $4, '{}'::jsonb, $5)",
    )
    .bind(Uuid::now_v7())
    .bind(agent)
    .bind(run)
    .bind(Uuid::now_v7())
    .bind(Utc::now())
    .execute(&pool)
    .await;
    assert!(
        with_mail.is_err(),
        "the refused mail has no row to point at"
    );
    pool.close().await;
}

#[tokio::test]
async fn the_same_source_leaves_one_item_at_an_address_and_another_address_keeps_its_own() {
    require_provisioned_infrastructure();
    let db = E2eDatabase::create(true, &[])
        .await
        .with_app_role(APP_ROLE, APP_PASSWORD)
        .await;
    let pool = PgPool::connect(&db.owner_migration_url())
        .await
        .expect("the owner connection opens");
    apply(&pool, THROUGH_THE_MAIL_NOTICE).await;
    let (job, run) = seed_run(&pool, &support::wire::unique_runner_type("source-key")).await;
    let (first, second) = (
        agent_of(&pool, run, job).await,
        agent_of(&pool, run, job).await,
    );
    let source = Uuid::now_v7().to_string();
    notice_item(&pool, "NOTICE", Some(first), Some(run), Some(&source))
        .await
        .expect("the first item inserts");
    assert!(
        notice_item(&pool, "MAIL_NOTICE", Some(first), Some(run), Some(&source))
            .await
            .is_err(),
        "the same source at the same address is refused"
    );
    notice_item(&pool, "NOTICE", Some(second), Some(run), Some(&source))
        .await
        .expect("another address keeps its own item");
    notice_item(&pool, "NOTICE", Some(first), Some(run), None)
        .await
        .expect("items with no source are never merged");
    notice_item(&pool, "NOTICE", Some(first), Some(run), None)
        .await
        .expect("items with no source are never merged");
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
