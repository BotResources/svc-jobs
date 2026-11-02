use async_trait::async_trait;
use bc_jobs::domain::ids::JobId;
use bc_jobs::event::job::{JOB_AGGREGATE_TYPE, JobEvent};
use bc_jobs::event::job_facts::JobContentErased;
use bc_jobs::ports::PortError;
use bc_jobs::ports::job::{CancelledRequestData, ContentErasure};
use chrono::{DateTime, Utc};
use sqlx::PgConnection;
use uuid::Uuid;

use crate::db::PgStore;
use crate::db::apply::apply_job_events;
use crate::db::hydrate::unavailable;
use crate::db::kept_payloads::lock_kept_payload;

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
    lock_kept_payload(&mut *tx, job_id).await?;
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

#[async_trait]
impl CancelledRequestData for PgStore {
    async fn cancelled_jobs_awaiting_erasure(
        &self,
        cancelled_before: DateTime<Utc>,
        batch: usize,
    ) -> Result<Vec<JobId>, PortError> {
        let ids: Vec<Uuid> = sqlx::query_scalar(
            "SELECT p.job_id::uuid FROM cancelled_request_data p \
             JOIN job_resolutions r ON r.job_id = p.job_id \
             WHERE r.kind = 'CANCELLED' AND r.occurred_at <= $1 \
             ORDER BY r.occurred_at, p.job_id LIMIT $2",
        )
        .bind(cancelled_before)
        .bind(i64::try_from(batch).unwrap_or(i64::MAX))
        .fetch_all(self.pool())
        .await
        .map_err(unavailable)?;
        ids.into_iter()
            .map(|id| JobId::new(id).map_err(PortError::from))
            .collect()
    }

    async fn erase_request_data_of(
        &self,
        job_id: JobId,
        erasure: ContentErasure<'_>,
    ) -> Result<bool, PortError> {
        let ContentErasure {
            at,
            event_id,
            metadata,
        } = erasure;
        let id = job_id;
        let job_id = job_id.as_uuid();
        let mut tx = self.begin().await?;
        lock_kept_payload(&mut tx, job_id).await?;
        let pending: Option<Uuid> = sqlx::query_scalar(
            "SELECT j.id::uuid FROM jobs j \
             WHERE j.id = $1 AND j.content_erased_at IS NULL \
               AND EXISTS (SELECT 1 FROM job_resolutions r \
                           WHERE r.job_id = j.id AND r.kind = 'CANCELLED') \
             FOR UPDATE OF j",
        )
        .bind(job_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?;
        if pending.is_some() {
            erase_request_data(&mut tx, job_id, at).await?;
            apply_job_events(
                &mut tx,
                id,
                &[(
                    event_id,
                    JobEvent::JobContentErased(JobContentErased { job_id: id, at }),
                )],
                metadata,
                at,
            )
            .await?;
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
