mod support;

use std::borrow::Cow;

use bc_jobs::domain::ids::RunId;
use bc_jobs::ports::transport::TriggerSequence;
use bc_jobs::ports::trigger_ledger::TriggerLedger;
use br_test_harness::E2eDatabase;
use chrono::Utc;
use sqlx::PgPool;
use sqlx::migrate::Migrator;
use support::fixture::{APP_PASSWORD, APP_ROLE, require_provisioned_infrastructure};
use svc_jobs::db::PgStore;
use uuid::Uuid;

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

const TRIGGER_SEQUENCE_MIGRATION: i64 = 15;

async fn apply_before_the_trigger_sequence(pool: &PgPool) {
    apply(pool, |version| version < TRIGGER_SEQUENCE_MIGRATION).await;
}

async fn apply_all(pool: &PgPool) {
    apply(pool, |_| true).await;
}

async fn apply(pool: &PgPool, keep: impl Fn(i64) -> bool) {
    let migrations = MIGRATOR
        .iter()
        .filter(|migration| keep(migration.version))
        .cloned()
        .collect::<Vec<_>>();
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

async fn seed_run(pool: &PgPool) -> Uuid {
    let (runner_type, job, run) = (Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7());
    sqlx::query("INSERT INTO runner_types (id, type_key) VALUES ($1, $2)")
        .bind(runner_type)
        .bind(format!("trigger-sequence-{}", runner_type.simple()))
        .execute(pool)
        .await
        .expect("the runner type inserts");
    sqlx::query(
        "INSERT INTO producers (id, bc_key) VALUES ($1, 'projects') ON CONFLICT (bc_key) DO NOTHING",
    )
    .bind(Uuid::now_v7())
    .execute(pool)
    .await
    .expect("the producer inserts");
    let producer: Uuid =
        sqlx::query_scalar("SELECT id::uuid FROM producers WHERE bc_key = 'projects'")
            .fetch_one(pool)
            .await
            .expect("the producer reads");
    sqlx::query(
        "INSERT INTO jobs (id, runner_type_id, producer_id, created_at) VALUES ($1, $2, $3, $4)",
    )
    .bind(job)
    .bind(runner_type)
    .bind(producer)
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
    run
}

async fn store(pool: &PgPool, run: Uuid, sequence: i64) -> bool {
    sqlx::query("INSERT INTO run_triggers (run_id, stream_sequence) VALUES ($1, $2)")
        .bind(run)
        .bind(sequence)
        .execute(pool)
        .await
        .is_ok()
}

#[tokio::test]
async fn runs_dispatched_before_the_migration_have_no_stored_sequence_and_new_ones_may_carry_one() {
    require_provisioned_infrastructure();
    let db = E2eDatabase::create(true, &[])
        .await
        .with_app_role(APP_ROLE, APP_PASSWORD)
        .await;
    let pool = PgPool::connect(&db.owner_migration_url())
        .await
        .expect("the owner connection opens");
    apply_before_the_trigger_sequence(&pool).await;
    let dispatched_before = seed_run(&pool).await;

    apply_all(&pool).await;

    let stored: i64 = sqlx::query_scalar("SELECT count(*) FROM run_triggers")
        .fetch_one(&pool)
        .await
        .expect("the table reads");
    assert_eq!(stored, 0, "a run dispatched before has no sequence");
    let dispatched_after = seed_run(&pool).await;
    assert!(store(&pool, dispatched_after, 41).await);
    assert!(
        store(&pool, dispatched_before, 41).await,
        "the broker numbers triggers across runner types: two runs of a recreated stream may share a sequence"
    );
    assert!(
        !store(&pool, dispatched_after, 42).await,
        "a run keeps one sequence"
    );
    assert!(
        !store(&pool, Uuid::now_v7(), 43).await,
        "a sequence is stored only for a run that exists"
    );
    let unseeded = seed_run(&pool).await;
    assert!(
        !store(&pool, unseeded, 0).await,
        "a stream sequence starts at one"
    );
    pool.close().await;
}

#[tokio::test]
async fn a_second_sequence_recorded_for_a_run_leaves_the_first_in_place() {
    require_provisioned_infrastructure();
    let db = E2eDatabase::create(true, &[])
        .await
        .with_app_role(APP_ROLE, APP_PASSWORD)
        .await;
    let pool = PgPool::connect(&db.owner_migration_url())
        .await
        .expect("the owner connection opens");
    apply_all(&pool).await;
    let run = seed_run(&pool).await;
    let store = PgStore::new(pool.clone());
    let run_id = RunId::new(run).expect("a v7 run id");

    for sequence in [41, 42] {
        store
            .record_trigger_sequence(run_id, TriggerSequence::new(sequence).expect("positive"))
            .await
            .expect("recording a sequence never fails on a repeat");
    }

    let kept: i64 =
        sqlx::query_scalar("SELECT stream_sequence FROM run_triggers WHERE run_id = $1")
            .bind(run)
            .fetch_one(&pool)
            .await
            .expect("the sequence reads");
    assert_eq!(kept, 41, "a run keeps the sequence it was dispatched under");
    pool.close().await;
}
