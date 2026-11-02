mod support;

use br_test_harness::E2eDatabase;
use chrono::Utc;
use sqlx::PgPool;
use sqlx::migrate::Migrator;
use support::fixture::{APP_PASSWORD, APP_ROLE, require_provisioned_infrastructure};
use svc_jobs::db::erasure::CANDIDATE_MAILS_SQL;
use uuid::Uuid;

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

const HISTORY: i64 = 100_000;

fn scans_of(plan: &serde_json::Value, found: &mut Vec<(String, String)>) {
    if let Some(node) = plan.get("Node Type").and_then(|node| node.as_str()) {
        let relation = plan
            .get("Relation Name")
            .and_then(|relation| relation.as_str())
            .unwrap_or("");
        found.push((node.to_owned(), relation.to_owned()));
    }
    if let Some(children) = plan.get("Plans").and_then(|plans| plans.as_array()) {
        for child in children {
            scans_of(child, found);
        }
    }
}

async fn seeded_history() -> (E2eDatabase, PgPool) {
    let db = E2eDatabase::create(true, &[])
        .await
        .with_app_role(APP_ROLE, APP_PASSWORD)
        .await;
    let pool = PgPool::connect(&db.owner_migration_url())
        .await
        .expect("the owner connection opens");
    MIGRATOR
        .run(&pool)
        .await
        .expect("the schema applies through the real migrator");
    let (runner_type_id, producer_id, job, run) = (
        Uuid::now_v7(),
        Uuid::now_v7(),
        Uuid::now_v7(),
        Uuid::now_v7(),
    );
    sqlx::query("INSERT INTO runner_types (id, type_key) VALUES ($1, $2)")
        .bind(runner_type_id)
        .bind(support::wire::unique_runner_type("erasure-candidates"))
        .execute(&pool)
        .await
        .expect("the runner type inserts");
    sqlx::query("INSERT INTO producers (id, bc_key) VALUES ($1, 'projects')")
        .bind(producer_id)
        .execute(&pool)
        .await
        .expect("the producer inserts");
    sqlx::query(
        "INSERT INTO jobs (id, runner_type_id, producer_id, created_at) VALUES ($1, $2, $3, $4)",
    )
    .bind(job)
    .bind(runner_type_id)
    .bind(producer_id)
    .bind(Utc::now())
    .execute(&pool)
    .await
    .expect("the job inserts");
    sqlx::query(
        "INSERT INTO runs (id, job_id, attempt_number, dispatched_at) VALUES ($1, $2, 1, $3)",
    )
    .bind(run)
    .bind(job)
    .bind(Utc::now())
    .execute(&pool)
    .await
    .expect("the run inserts");
    sqlx::query(
        "INSERT INTO mails (mail_id, sender_address, sender_run_id, initial_job_id, author, \
            source_conversation_id, subject, body, created_at) \
         SELECT gen_random_uuid(), gen_random_uuid(), $1, $2, 'author', gen_random_uuid(), \
            'subject', 'body', now() FROM generate_series(1, $3)",
    )
    .bind(run)
    .bind(job)
    .bind(HISTORY)
    .execute(&pool)
    .await
    .expect("the mails insert");
    sqlx::query(
        "INSERT INTO mail_recipients (mail_id, address, kind) \
         SELECT mail_id, gen_random_uuid(), 'DIRECT' FROM mails",
    )
    .execute(&pool)
    .await
    .expect("the recipients insert");
    sqlx::query(
        "INSERT INTO directory_removed_addresses (agent_id, removed_at) \
         SELECT gen_random_uuid(), now() FROM generate_series(1, $1)",
    )
    .bind(HISTORY)
    .execute(&pool)
    .await
    .expect("the tombstones insert");
    sqlx::query("ANALYZE")
        .execute(&pool)
        .await
        .expect("the statistics refresh");
    (db, pool)
}

#[tokio::test]
async fn the_mails_a_removal_can_erase_are_found_through_the_address_indexes() {
    require_provisioned_infrastructure();
    let (_db, pool) = seeded_history().await;
    let removed: Vec<Uuid> = sqlx::query_scalar("SELECT sender_address FROM mails LIMIT 3")
        .fetch_all(&pool)
        .await
        .expect("three senders read");
    sqlx::query(
        "INSERT INTO directory_removed_addresses (agent_id, removed_at) \
         SELECT unnest($1::uuid[]), now()",
    )
    .bind(&removed)
    .execute(&pool)
    .await
    .expect("the removed addresses insert");
    let mut tx = pool.begin().await.expect("a transaction opens");

    let plan: serde_json::Value =
        sqlx::query_scalar(&format!("EXPLAIN (FORMAT JSON) {CANDIDATE_MAILS_SQL}"))
            .bind(&removed)
            .fetch_one(&mut *tx)
            .await
            .expect("the plan reads");

    let mut nodes = Vec::new();
    scans_of(&plan[0]["Plan"], &mut nodes);
    let scanned_whole: Vec<_> = nodes
        .iter()
        .filter(|(node, relation)| {
            node == "Seq Scan"
                && ["mails", "mail_recipients", "directory_removed_addresses"]
                    .contains(&relation.as_str())
        })
        .collect();
    assert!(
        scanned_whole.is_empty(),
        "the candidates are found by address, never by walking the history or the tombstones: {nodes:?}"
    );
    tx.rollback().await.expect("the transaction ends");
    pool.close().await;
}
