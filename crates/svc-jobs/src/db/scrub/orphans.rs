use bc_jobs::ports::PortError;
use bc_jobs::ports::scrub::OrphanUploads;
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use crate::db::hydrate::unavailable;
use crate::db::kept_payloads::try_lock_kept_payload;
use crate::db::mail::try_lock_mail_attachments;

pub async fn erase_orphan_uploads(
    pool: &PgPool,
    uploaded_before: DateTime<Utc>,
    batch: usize,
) -> Result<OrphanUploads, PortError> {
    let limit = i64::try_from(batch).unwrap_or(i64::MAX);
    Ok(OrphanUploads {
        payloads: erase_payloads(pool, uploaded_before, limit).await?,
        attachments: erase_attachments(pool, uploaded_before, limit).await?,
    })
}

async fn erase_payloads(
    pool: &PgPool,
    uploaded_before: DateTime<Utc>,
    limit: i64,
) -> Result<u64, PortError> {
    let candidates: Vec<Uuid> = sqlx::query_scalar(
        "SELECT k.job_id::uuid FROM kept_payloads k \
         WHERE k.uploaded_at <= $1 \
           AND NOT EXISTS (SELECT 1 FROM jobs j WHERE j.id = k.job_id) \
         ORDER BY k.uploaded_at, k.job_id LIMIT $2",
    )
    .bind(uploaded_before)
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(unavailable)?;
    let mut erased = 0;
    for job_id in candidates {
        let mut tx = pool.begin().await.map_err(unavailable)?;
        if !try_lock_kept_payload(&mut tx, job_id).await? {
            continue;
        }
        erased += sqlx::query(
            "DELETE FROM kept_payloads k WHERE k.job_id = $1 AND k.uploaded_at <= $2 \
               AND NOT EXISTS (SELECT 1 FROM jobs j WHERE j.id = k.job_id)",
        )
        .bind(job_id)
        .bind(uploaded_before)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?
        .rows_affected();
        tx.commit().await.map_err(unavailable)?;
    }
    Ok(erased)
}

async fn erase_attachments(
    pool: &PgPool,
    uploaded_before: DateTime<Utc>,
    limit: i64,
) -> Result<u64, PortError> {
    let candidates: Vec<Uuid> = sqlx::query_scalar(
        "SELECT x.mail_id FROM mail_attachments x \
         WHERE x.uploaded_at <= $1 \
           AND NOT EXISTS (SELECT 1 FROM mails m WHERE m.mail_id = x.mail_id) \
         GROUP BY x.mail_id ORDER BY min(x.uploaded_at), x.mail_id LIMIT $2",
    )
    .bind(uploaded_before)
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(unavailable)?;
    let mut erased = 0;
    for mail_id in candidates {
        let mut tx = pool.begin().await.map_err(unavailable)?;
        if !try_lock_mail_attachments(&mut tx, mail_id).await? {
            continue;
        }
        erased += sqlx::query(
            "DELETE FROM mail_attachments a WHERE a.mail_id = $1 AND a.uploaded_at <= $2 \
               AND NOT EXISTS (SELECT 1 FROM mails m WHERE m.mail_id = a.mail_id)",
        )
        .bind(mail_id)
        .bind(uploaded_before)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?
        .rows_affected();
        tx.commit().await.map_err(unavailable)?;
    }
    Ok(erased)
}
