use bc_jobs::domain::ids::{AgentId, ItemId, JobId, RunId};
use bc_jobs::domain::mail::content::MailContent;
use bc_jobs::domain::mail::delivery::RecipientKind;
use bc_jobs::domain::mail::inbox::ItemKind;
use bc_jobs::ports::PortError;
use chrono::{DateTime, Utc};
use sqlx::PgConnection;
use uuid::Uuid;

use crate::db::hydrate::unavailable;

pub(super) async fn insert_mail(
    tx: &mut PgConnection,
    content: &MailContent,
    run_id: RunId,
    initial_job_id: JobId,
    at: DateTime<Utc>,
) -> Result<bool, PortError> {
    let inserted = sqlx::query(
        "INSERT INTO mails (mail_id, sender_address, sender_run_id, initial_job_id, author, \
            source_conversation_id, subject, body, created_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) ON CONFLICT (mail_id) DO NOTHING",
    )
    .bind(content.mail_id.as_uuid())
    .bind(content.from.as_uuid())
    .bind(run_id.as_uuid())
    .bind(initial_job_id.as_uuid())
    .bind(content.author.as_str())
    .bind(content.source_conversation.as_uuid())
    .bind(&content.subject)
    .bind(&content.body)
    .bind(at)
    .execute(&mut *tx)
    .await
    .map_err(unavailable)?
    .rows_affected();
    if inserted == 0 {
        return Ok(false);
    }
    for (address, kind) in content
        .to
        .iter()
        .map(|address| (address, RecipientKind::Direct))
        .chain(
            content
                .cc
                .iter()
                .map(|address| (address, RecipientKind::Cc)),
        )
    {
        sqlx::query("INSERT INTO mail_recipients (mail_id, address, kind) VALUES ($1, $2, $3)")
            .bind(content.mail_id.as_uuid())
            .bind(address.as_uuid())
            .bind(kind.as_db_str())
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
    }
    let named: Vec<Uuid> = content
        .attachments
        .iter()
        .map(|attachment| attachment.attachment_id.as_uuid())
        .collect();
    sqlx::query(
        "UPDATE mail_attachments SET listed = true WHERE mail_id = $1 AND attachment_id = ANY($2)",
    )
    .bind(content.mail_id.as_uuid())
    .bind(&named)
    .execute(&mut *tx)
    .await
    .map_err(unavailable)?;
    Ok(true)
}

pub async fn insert_item(
    tx: &mut PgConnection,
    item_id: ItemId,
    kind: ItemKind,
    address: AgentId,
    mail_id: Uuid,
    at: DateTime<Utc>,
) -> Result<(), PortError> {
    sqlx::query(
        "INSERT INTO inbox_items (item_id, kind, address, mail_id, accepted_at) \
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(item_id.as_uuid())
    .bind(kind.as_db_str())
    .bind(address.as_uuid())
    .bind(mail_id)
    .bind(at)
    .execute(&mut *tx)
    .await
    .map_err(unavailable)?;
    Ok(())
}
