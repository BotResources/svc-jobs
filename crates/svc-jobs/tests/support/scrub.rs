use std::time::Duration;

use serde_json::{Value, json};
use uuid::Uuid;

use super::LONG;
use super::db::Durable;
use super::gesture::Work;

pub const SCRUBBED_MARK: &str = "SELECT count(*) FROM tree_scrubs WHERE initial_job_id = $1";

pub const SCRUBBED_ACCESS_PATH: &str =
    "SELECT count(*) FROM jobs WHERE id = $1 AND tree_scrubbed_at IS NOT NULL";

const RUNS_OF_TREE: &str =
    "SELECT r.id FROM runs r WHERE r.job_id IN (SELECT id FROM jobs WHERE root_job_id = $1)";

const AGING: [(&str, &str, &str); 6] = [
    (
        "jobs",
        "created_at",
        "id IN (SELECT id FROM jobs WHERE root_job_id = $1)",
    ),
    (
        "runs",
        "dispatched_at",
        "job_id IN (SELECT id FROM jobs WHERE root_job_id = $1)",
    ),
    ("run_starts", "started_at", "run_id IN (RUNS)"),
    ("run_terminals", "occurred_at", "run_id IN (RUNS)"),
    (
        "domain_events",
        "occurred_at",
        "aggregate_id IN (SELECT id FROM jobs WHERE root_job_id = $1)",
    ),
    (
        "job_resolutions",
        "occurred_at",
        "job_id IN (SELECT id FROM jobs WHERE root_job_id = $1)",
    ),
];

pub async fn age_tree(durable: &Durable, root: Uuid, days: i64) {
    let days = i32::try_from(days).expect("days fit");
    for (table, column, owned) in AGING {
        let owned = owned.replace("RUNS", RUNS_OF_TREE);
        let sql = format!(
            "UPDATE {table} SET {column} = {column} - make_interval(days => $2::int) \
             WHERE {owned}"
        );
        sqlx::query(&sql)
            .bind(root)
            .bind(days)
            .execute(durable.pool())
            .await
            .unwrap_or_else(|error| panic!("aging {table}.{column}: {error}"));
    }
}

pub async fn age_retry_schedules(durable: &Durable, root: Uuid, days: i64) {
    let days = i32::try_from(days).expect("days fit");
    sqlx::query(
        "UPDATE run_retry_schedules SET due_at = due_at - make_interval(days => $2::int) \
         WHERE failed_run_id IN (SELECT r.id FROM runs r \
             WHERE r.job_id IN (SELECT id FROM jobs WHERE root_job_id = $1))",
    )
    .bind(root)
    .bind(days)
    .execute(durable.pool())
    .await
    .unwrap_or_else(|error| panic!("aging run_retry_schedules.due_at: {error}"));
}

pub async fn age_orphan_uploads(durable: &Durable, days: i64) {
    let days = i32::try_from(days).expect("days fit");
    for (table, column) in [
        ("kept_payloads", "uploaded_at"),
        ("mail_attachments", "uploaded_at"),
    ] {
        sqlx::query(&format!(
            "UPDATE {table} SET {column} = {column} - make_interval(days => $1::int)"
        ))
        .bind(days)
        .execute(durable.pool())
        .await
        .unwrap_or_else(|error| panic!("aging {table}.{column}: {error}"));
    }
}

pub async fn await_scrubbed(durable: &Durable, root: Uuid, what: &str) {
    durable.await_count(SCRUBBED_MARK, root, 1, what).await;
    durable
        .assert_count(SCRUBBED_ACCESS_PATH, root, 1, what)
        .await;
}

pub async fn assert_not_scrubbed_for(durable: &Durable, root: Uuid, wait: Duration, what: &str) {
    let waited = tokio::time::Instant::now();
    while waited.elapsed() < wait {
        durable.assert_count(SCRUBBED_MARK, root, 0, what).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(wait < LONG, "{what}: the quiet wait stays short");
}

pub fn trigger_of(work: &Work) -> Value {
    json!({
        "run_id": work.run_id.to_string(),
        "job_id": work.job_id.to_string(),
        "attempt": 1,
    })
}
