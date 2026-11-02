use std::time::Duration;

use bc_jobs::domain::ids::JobId;
use bc_jobs::domain::ownership::ActorRef;
use bc_jobs::domain::runner::kept_payload::KeptPayload;
use bc_jobs::ports::kept_payload::{KeptPayloads, Uploader};
use chrono::TimeDelta;
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use super::clock;
use super::contention::Fixture;

pub const ORPHAN_AGE_DAYS: i64 = 40;

pub fn uploader() -> ActorRef {
    ActorRef::new(Uuid::from_u128(0x7e57_0000_0000_7000_8000_0000_0000_0001))
}

pub async fn an_old_kept_payload(fixture: &Fixture, job_id: JobId) {
    let admission = fixture
        .store
        .upload_payload(
            Uploader::Service(uploader()),
            job_id.as_uuid(),
            KeptPayload {
                message: "an upload".to_owned(),
                attachments: vec![],
            },
            clock::now() - TimeDelta::days(ORPHAN_AGE_DAYS),
        )
        .await
        .expect("the upload lands")
        .expect("the upload is admitted");
    assert_eq!(
        admission,
        bc_jobs::domain::runner::kept_payload::UploadAdmission::Store
    );
}

pub async fn an_old_unlisted_attachment(fixture: &Fixture, mail_id: Uuid) {
    sqlx::query(
        "INSERT INTO mail_attachments \
            (mail_id, attachment_id, uploader_address, name, content_type, content, size, \
             uploaded_at) \
         VALUES ($1, $2, $3, 'a.txt', 'text/plain', 'x', 1, $4)",
    )
    .bind(mail_id)
    .bind(Uuid::now_v7())
    .bind(Uuid::now_v7())
    .bind(clock::now() - TimeDelta::days(ORPHAN_AGE_DAYS))
    .execute(&fixture.pool)
    .await
    .expect("the attachment inserts");
}

pub async fn holding(fixture: &Fixture) -> Transaction<'static, Postgres> {
    fixture.pool.begin().await.expect("a transaction opens")
}

pub async fn await_a_writer_blocked_on_an_advisory_lock(pool: &PgPool) {
    for _ in 0..200 {
        let waiting: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid \
             WHERE l.locktype = 'advisory' AND NOT l.granted AND a.datname = current_database()",
        )
        .fetch_one(pool)
        .await
        .expect("the lock-wait probe runs");
        if waiting > 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!(
        "no writer ever waited on the advisory lock, so the interleaving this scenario needs \
         never happened and it would prove nothing"
    );
}
