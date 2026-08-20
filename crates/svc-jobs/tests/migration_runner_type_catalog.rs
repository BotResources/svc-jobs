mod support;

use bc_jobs::domain::keys::RunnerTypeKey;
use br_test_harness::E2eDatabase;
use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row};
use svc_jobs::db::apply::refs;
use uuid::Uuid;

use support::fixture::require_provisioned_infrastructure;

#[tokio::test]
async fn legacy_runner_types_are_backfilled_as_active_without_breaking_references() {
    require_provisioned_infrastructure();
    let db = E2eDatabase::create(true, &[]).await;
    let pool = PgPool::connect(&db.owner_migration_url())
        .await
        .expect("the owner connection opens");

    sqlx::raw_sql(include_str!("../migrations/0001_jobs.sql"))
        .execute(&pool)
        .await
        .expect("the 0.1 schema applies");

    let presence_type_id = Uuid::now_v7();
    let job_type_id = Uuid::now_v7();
    let instance_id = Uuid::now_v7();
    let session_id = Uuid::now_v7();
    let producer_id = Uuid::now_v7();
    let job_id = Uuid::now_v7();
    let connected_at = "2025-01-02T03:04:05Z"
        .parse::<DateTime<Utc>>()
        .expect("a fixed connection time");
    let job_created_at = "2025-02-03T04:05:06Z"
        .parse::<DateTime<Utc>>()
        .expect("a fixed job creation time");

    sqlx::query(
        "INSERT INTO runner_types (id, type_key) VALUES ($1, 'presence'), ($2, 'job-only')",
    )
    .bind(presence_type_id)
    .bind(job_type_id)
    .execute(&pool)
    .await
    .expect("legacy runner types insert");
    sqlx::query(
        "INSERT INTO runner_instances (id, runner_type_id, instance_key) \
         VALUES ($1, $2, 'presence-1')",
    )
    .bind(instance_id)
    .bind(presence_type_id)
    .execute(&pool)
    .await
    .expect("the legacy instance inserts");
    sqlx::query(
        "INSERT INTO runner_presence_sessions \
         (id, instance_id, version, connected_at, last_observed_at) \
         VALUES ($1, $2, '0.1.0', $3, $3)",
    )
    .bind(session_id)
    .bind(instance_id)
    .bind(connected_at)
    .execute(&pool)
    .await
    .expect("the legacy presence inserts");
    sqlx::query("INSERT INTO producers (id, bc_key) VALUES ($1, 'projects')")
        .bind(producer_id)
        .execute(&pool)
        .await
        .expect("the legacy producer inserts");
    sqlx::query(
        "INSERT INTO jobs (id, runner_type_id, producer_id, created_at) \
         VALUES ($1, $2, $3, $4)",
    )
    .bind(job_id)
    .bind(job_type_id)
    .bind(producer_id)
    .bind(job_created_at)
    .execute(&pool)
    .await
    .expect("the legacy job inserts");

    sqlx::raw_sql(include_str!("../migrations/0002_runner_type_catalog.sql"))
        .execute(&pool)
        .await
        .expect("the 0.2 migration applies over legacy data");

    let rows = sqlx::query(
        "SELECT rt.type_key, registered.lifecycle::text AS lifecycle, registered.registered_at \
         FROM registered_runner_types registered \
         JOIN runner_types rt ON rt.id = registered.runner_type_id ORDER BY rt.type_key",
    )
    .fetch_all(&pool)
    .await
    .expect("the migrated catalog reads");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<String, _>("type_key"), "presence");
    assert_eq!(rows[0].get::<String, _>("lifecycle"), "ACTIVE");
    assert_eq!(
        rows[0].get::<DateTime<Utc>, _>("registered_at"),
        connected_at
    );
    let job_only_registered: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM registered_runner_types WHERE runner_type_id = $1)",
    )
    .bind(job_type_id)
    .fetch_one(&pool)
    .await
    .expect("the job-only route can be distinguished from a registered aggregate");
    assert!(
        !job_only_registered,
        "historical routing alone must not invent a RunnerType entity"
    );

    let instance_backref: Uuid =
        sqlx::query_scalar("SELECT runner_type_id FROM runner_instances WHERE id = $1")
            .bind(instance_id)
            .fetch_one(&pool)
            .await
            .expect("the instance back-reference survives");
    let job_backref: Uuid = sqlx::query_scalar("SELECT runner_type_id FROM jobs WHERE id = $1")
        .bind(job_id)
        .fetch_one(&pool)
        .await
        .expect("the job back-reference survives");
    assert_eq!(instance_backref, presence_type_id);
    assert_eq!(job_backref, job_type_id);

    let mut routing_tx = pool.begin().await.expect("a routing transaction opens");
    let runtime_route = refs::runner_type_id(
        &mut routing_tx,
        &RunnerTypeKey::new("runtime-job-only").unwrap(),
    )
    .await
    .expect("job routing accepts an unregistered key");
    routing_tx.commit().await.unwrap();
    let runtime_registered: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM registered_runner_types WHERE runner_type_id = $1)",
    )
    .bind(runtime_route)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        !runtime_registered,
        "resolving a Job route at runtime must not register a RunnerType aggregate"
    );

    pool.close().await;
    db.cleanup().await;
}
