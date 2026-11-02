use bc_jobs::domain::erasure::{ERASED_CONVERSATION_ID, ERASED_TEXT};
use bc_jobs::domain::job::Job;
use bc_jobs::event::job::JOB_AGGREGATE_TYPE;
use bc_jobs::ports::PortError;
use chrono::{DateTime, Utc};
use sqlx::PgConnection;
use uuid::Uuid;

use super::PgStore;
use super::hydrate::unavailable;

const ERASABLE_MAILS: &str = "\
    WITH erasable AS ( \
        SELECT m.mail_id FROM mails m \
        WHERE m.content_erased_at IS NULL \
          AND (m.sender_address = ANY($1) \
               OR EXISTS (SELECT 1 FROM mail_recipients r \
                          WHERE r.mail_id = m.mail_id AND r.address = ANY($1))) \
          AND EXISTS (SELECT 1 FROM directory_removed_addresses d \
                      WHERE d.agent_id = m.sender_address) \
          AND NOT EXISTS (SELECT 1 FROM mail_recipients r \
                          WHERE r.mail_id = m.mail_id \
                            AND NOT EXISTS (SELECT 1 FROM directory_removed_addresses d \
                                            WHERE d.agent_id = r.address))), \
     blanked AS ( \
        UPDATE mails SET author = $3, source_conversation_id = $4, subject = '', body = '', \
            content_erased_at = $2 \
        WHERE mail_id IN (SELECT mail_id FROM erasable)) \
     DELETE FROM mail_attachments WHERE mail_id IN (SELECT mail_id FROM erasable)";

pub async fn erase_mail_content_between_removed_addresses(
    tx: &mut PgConnection,
    removed: &[Uuid],
    at: DateTime<Utc>,
) -> Result<(), PortError> {
    if removed.is_empty() {
        return Ok(());
    }
    sqlx::query(ERASABLE_MAILS)
        .bind(removed)
        .bind(at)
        .bind(ERASED_TEXT)
        .bind(ERASED_CONVERSATION_ID)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
    Ok(())
}

pub async fn erase_results_of_job(tx: &mut PgConnection, job_id: Uuid) -> Result<(), PortError> {
    sqlx::query("DELETE FROM job_results WHERE job_id = $1")
        .bind(job_id)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
    Ok(())
}

pub async fn erase_request_data(
    tx: &mut PgConnection,
    job_id: Uuid,
    at: DateTime<Utc>,
) -> Result<(), PortError> {
    sqlx::query(
        "UPDATE jobs SET config = NULL, passport = NULL, payload = NULL, \
            content_erased_at = coalesce(content_erased_at, $2) \
         WHERE id = $1",
    )
    .bind(job_id)
    .bind(at)
    .execute(&mut *tx)
    .await
    .map_err(unavailable)?;
    sqlx::query("DELETE FROM kept_payloads WHERE job_id = $1")
        .bind(job_id)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
    sqlx::query(
        "UPDATE domain_events SET payload = payload - 'config' - 'passport' - 'payload' \
         WHERE aggregate_type = $1 AND aggregate_id = $2 AND event_type = 'JobQueued'",
    )
    .bind(JOB_AGGREGATE_TYPE)
    .bind(job_id)
    .execute(&mut *tx)
    .await
    .map_err(unavailable)?;
    sqlx::query("DELETE FROM cancelled_request_data WHERE job_id = $1")
        .bind(job_id)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
    Ok(())
}

pub async fn await_request_data_erasure(
    tx: &mut PgConnection,
    job_id: Uuid,
) -> Result<(), PortError> {
    sqlx::query("INSERT INTO cancelled_request_data (job_id) VALUES ($1) ON CONFLICT DO NOTHING")
        .bind(job_id)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
    Ok(())
}

impl PgStore {
    pub async fn cancelled_jobs_awaiting_erasure(
        &self,
        cancelled_before: DateTime<Utc>,
        batch: i64,
    ) -> Result<Vec<Job>, PortError> {
        let ids: Vec<Uuid> = sqlx::query_scalar(
            "SELECT p.job_id::uuid FROM cancelled_request_data p \
             JOIN job_resolutions r ON r.job_id = p.job_id \
             WHERE r.occurred_at <= $1 ORDER BY r.occurred_at LIMIT $2",
        )
        .bind(cancelled_before)
        .bind(batch)
        .fetch_all(self.pool())
        .await
        .map_err(unavailable)?;
        self.load_batch(&ids).await
    }

    pub async fn erase_request_data_of(
        &self,
        job_id: Uuid,
        at: DateTime<Utc>,
    ) -> Result<bool, PortError> {
        let mut tx = self.begin().await?;
        let pending: Option<Uuid> = sqlx::query_scalar(
            "SELECT id::uuid FROM jobs WHERE id = $1 AND content_erased_at IS NULL FOR UPDATE",
        )
        .bind(job_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?;
        if pending.is_some() {
            erase_request_data(&mut tx, job_id, at).await?;
        } else {
            sqlx::query("DELETE FROM cancelled_request_data WHERE job_id = $1")
                .bind(job_id)
                .execute(&mut *tx)
                .await
                .map_err(unavailable)?;
        }
        tx.commit().await.map_err(unavailable)?;
        Ok(pending.is_some())
    }
}
