mod support;

use std::time::Duration;

use br_test_harness::E2eDatabase;
use chrono::Utc;
use sqlx::PgPool;
use sqlx::migrate::Migrator;
use support::fixture::{APP_PASSWORD, APP_ROLE, require_provisioned_infrastructure};
use svc_jobs::db::erasure::erase_mail_content_between_removed_addresses;
use uuid::Uuid;

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

async fn mail_between_removed_addresses(pool: &PgPool) -> (Uuid, Uuid) {
    let (runner_type_id, producer_id, job, run) = (
        Uuid::now_v7(),
        Uuid::now_v7(),
        Uuid::now_v7(),
        Uuid::now_v7(),
    );
    let (mail, sender, recipient) = (Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7());
    sqlx::query("INSERT INTO runner_types (id, type_key) VALUES ($1, $2)")
        .bind(runner_type_id)
        .bind(support::wire::unique_runner_type("erasure-race"))
        .execute(pool)
        .await
        .expect("the runner type inserts");
    sqlx::query("INSERT INTO producers (id, bc_key) VALUES ($1, 'projects')")
        .bind(producer_id)
        .execute(pool)
        .await
        .expect("the producer inserts");
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
    sqlx::query(
        "INSERT INTO mails (mail_id, sender_address, sender_run_id, initial_job_id, author, \
            source_conversation_id, subject, body, created_at) \
         VALUES ($1, $2, $3, $4, 'author', $5, 'subject', 'body', now())",
    )
    .bind(mail)
    .bind(sender)
    .bind(run)
    .bind(job)
    .bind(Uuid::now_v7())
    .execute(pool)
    .await
    .expect("the mail inserts");
    sqlx::query("INSERT INTO mail_recipients (mail_id, address, kind) VALUES ($1, $2, 'DIRECT')")
        .bind(mail)
        .bind(recipient)
        .execute(pool)
        .await
        .expect("the recipient inserts");
    sqlx::query(
        "INSERT INTO directory_removed_addresses (agent_id, removed_at) \
         SELECT unnest($1::uuid[]), now()",
    )
    .bind(vec![sender, recipient])
    .execute(pool)
    .await
    .expect("the tombstones insert");
    (mail, recipient)
}

#[tokio::test]
async fn an_attachment_stored_while_a_mail_is_being_erased_does_not_survive_the_erasure() {
    require_provisioned_infrastructure();
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
    let (mail, recipient) = mail_between_removed_addresses(&pool).await;

    let mut upload = pool.begin().await.expect("the upload transaction opens");
    let erased: bool = sqlx::query_scalar(
        "SELECT content_erased_at IS NOT NULL FROM mails WHERE mail_id = $1 FOR SHARE",
    )
    .bind(mail)
    .fetch_one(&mut *upload)
    .await
    .expect("the upload reads the mail");
    assert!(!erased, "the upload sees a mail that is not erased yet");

    let eraser_pool = pool.clone();
    let eraser = tokio::spawn(async move {
        let mut tx = eraser_pool.begin().await.expect("the removal opens");
        erase_mail_content_between_removed_addresses(&mut tx, &[recipient], Utc::now())
            .await
            .expect("the erasure runs");
        tx.commit().await.expect("the removal commits");
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        !eraser.is_finished(),
        "the erasure waits for the upload that read the mail first"
    );
    sqlx::query(
        "INSERT INTO mail_attachments (mail_id, attachment_id, uploader_address, name, \
            content_type, content, size, uploaded_at) \
         VALUES ($1, $2, $3, 'late.txt', 'text/plain', 'late', 4, now())",
    )
    .bind(mail)
    .bind(Uuid::now_v7())
    .bind(Uuid::now_v7())
    .execute(&mut *upload)
    .await
    .expect("the attachment inserts");
    upload.commit().await.expect("the upload commits");
    eraser.await.expect("the removal ends");

    let left: i64 = sqlx::query_scalar("SELECT count(*) FROM mail_attachments WHERE mail_id = $1")
        .bind(mail)
        .fetch_one(&pool)
        .await
        .expect("the attachments count");
    assert_eq!(left, 0, "the erased mail holds no attachment");
    let stamped: bool =
        sqlx::query_scalar("SELECT content_erased_at IS NOT NULL FROM mails WHERE mail_id = $1")
            .bind(mail)
            .fetch_one(&pool)
            .await
            .expect("the mail reads");
    assert!(stamped);
    pool.close().await;
}
