use bc_jobs::ports::PortError;
use bc_jobs::ports::scrub::OrphanUploads;
use chrono::{DateTime, Utc};
use sqlx::PgPool;

use crate::db::hydrate::unavailable;

pub async fn erase_orphan_uploads(
    pool: &PgPool,
    uploaded_before: DateTime<Utc>,
    batch: usize,
) -> Result<OrphanUploads, PortError> {
    let limit = i64::try_from(batch).unwrap_or(i64::MAX);
    let payloads = sqlx::query(
        "DELETE FROM kept_payloads WHERE job_id IN ( \
            SELECT k.job_id FROM kept_payloads k \
            WHERE k.uploaded_at <= $1 \
              AND NOT EXISTS (SELECT 1 FROM jobs j WHERE j.id = k.job_id) \
            ORDER BY k.uploaded_at, k.job_id LIMIT $2)",
    )
    .bind(uploaded_before)
    .bind(limit)
    .execute(pool)
    .await
    .map_err(unavailable)?
    .rows_affected();
    let attachments = sqlx::query(
        "DELETE FROM mail_attachments a USING ( \
            SELECT x.mail_id, x.attachment_id FROM mail_attachments x \
            WHERE x.uploaded_at <= $1 \
              AND NOT EXISTS (SELECT 1 FROM mails m WHERE m.mail_id = x.mail_id) \
            ORDER BY x.uploaded_at, x.mail_id, x.attachment_id LIMIT $2) doomed \
         WHERE a.mail_id = doomed.mail_id AND a.attachment_id = doomed.attachment_id",
    )
    .bind(uploaded_before)
    .bind(limit)
    .execute(pool)
    .await
    .map_err(unavailable)?
    .rows_affected();
    Ok(OrphanUploads {
        payloads,
        attachments,
    })
}
