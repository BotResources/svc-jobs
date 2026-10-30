mod support;

use std::borrow::Cow;

use br_test_harness::E2eDatabase;
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use sqlx::PgPool;
use sqlx::migrate::Migrator;
use uuid::Uuid;

use support::fixture::{
    APP_PASSWORD, APP_ROLE, JobsFixture, Knobs, require_provisioned_infrastructure,
};
use support::runner::{self, FakeRunner};
use support::{LONG, gql, wire};

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

const PRE_0_6_MIGRATIONS: usize = 3;

#[tokio::test]
async fn a_job_from_before_0_6_is_its_own_parent_and_initial_job_and_still_runs() {
    require_provisioned_infrastructure();
    let db = E2eDatabase::create(true, &[])
        .await
        .with_app_role(APP_ROLE, APP_PASSWORD)
        .await;
    let pool = PgPool::connect(&db.owner_migration_url())
        .await
        .expect("the owner connection opens");
    apply_the_first_migrations(&pool, PRE_0_6_MIGRATIONS).await;
    let legacy_type = wire::unique_runner_type("legacy-requester");
    let job_id = seed_a_job_waiting_before_0_6(&pool, &legacy_type).await;
    apply_the_first_migrations(&pool, MIGRATOR.iter().count()).await;
    let stored = sqlx::query_as::<_, (bool, bool, bool, bool)>(
        "SELECT requester IS NULL, passport IS NULL, payload IS NULL, call IS NULL \
         FROM jobs WHERE id = $1",
    )
    .bind(job_id)
    .fetch_one(&pool)
    .await
    .expect("the legacy job survives the migration");
    assert_eq!(
        stored,
        (true, true, true, true),
        "the migration adds the four columns empty and invents nothing for a job that predates them"
    );
    pool.close().await;

    let fixture = JobsFixture::start_on(
        db,
        Knobs {
            backstop_interval_seconds: 1,
            ..Knobs::default()
        },
    )
    .await;
    let client = fixture.gql();
    let admin = fixture.admin().clone();

    let view = gql::job(&client, &admin, job_id).await;
    assert_eq!(view["status"], json!("PENDING"));
    assert_eq!(
        view["parentJobId"],
        Value::Null,
        "a job of a Tree that predates the Initial Job is read as it was stored"
    );

    let mut instance = FakeRunner::new(fixture.nats(), &legacy_type, "instance-a");
    instance.connect().await;
    let trigger = instance.next_trigger(LONG).await;
    assert_eq!(runner::job_id(&trigger), job_id);
    assert_eq!(trigger["parent_job_id"], json!(job_id.to_string()));
    assert_eq!(trigger["initial_job_id"], json!(job_id.to_string()));
    assert_eq!(trigger["passport"], Value::Null);
    instance.start_run(&trigger).await;
    gql::wait_for_status(&client, &admin, job_id, "IN_PROGRESS", LONG).await;

    fixture.shutdown().await;
}

async fn apply_the_first_migrations(pool: &PgPool, count: usize) {
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

async fn seed_a_job_waiting_before_0_6(pool: &PgPool, type_key: &str) -> Uuid {
    let runner_type_id = Uuid::now_v7();
    let producer_id = Uuid::now_v7();
    let job_id = Uuid::now_v7();
    let created_at = "2025-01-02T03:04:05Z"
        .parse::<DateTime<Utc>>()
        .expect("a fixed creation time");
    sqlx::query("INSERT INTO runner_types (id, type_key) VALUES ($1, $2)")
        .bind(runner_type_id)
        .bind(type_key)
        .execute(pool)
        .await
        .expect("the legacy runner type inserts");
    sqlx::query("INSERT INTO producers (id, bc_key) VALUES ($1, 'projects')")
        .bind(producer_id)
        .execute(pool)
        .await
        .expect("the legacy producer inserts");
    sqlx::query(
        "INSERT INTO jobs (id, runner_type_id, producer_id, created_at, root_job_id) \
         VALUES ($1, $2, $3, $4, NULL)",
    )
    .bind(job_id)
    .bind(runner_type_id)
    .bind(producer_id)
    .bind(created_at)
    .execute(pool)
    .await
    .expect("the legacy job inserts");
    job_id
}
