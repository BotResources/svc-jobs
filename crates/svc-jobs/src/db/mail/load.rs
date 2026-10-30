use std::collections::HashMap;

use bc_jobs::domain::ids::{AgentId, AttachmentId, JobId, MailId};
use bc_jobs::domain::mail::author::{MailAuthor, SourceConversation};
use bc_jobs::domain::mail::content::{AttachmentRef, MailContent};
use bc_jobs::domain::mail::delivery::{AddressStanding, HeldAddress};
use bc_jobs::domain::mail::views::MailView;
use bc_jobs::ports::PortError;
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use crate::db::directory_entry::{ENTRY_TABLES, entry_columns, entry_of};
use crate::db::hydrate::unavailable;

pub async fn load_mail(
    tx: &mut PgConnection,
    mail_id: MailId,
) -> Result<Option<MailView>, PortError> {
    let Some(row) = sqlx::query(
        "SELECT sender_address, author, source_conversation_id, subject, body, created_at \
         FROM mails WHERE mail_id = $1",
    )
    .bind(mail_id.as_uuid())
    .fetch_optional(&mut *tx)
    .await
    .map_err(unavailable)?
    else {
        return Ok(None);
    };
    let recipients = sqlx::query("SELECT address, kind FROM mail_recipients WHERE mail_id = $1")
        .bind(mail_id.as_uuid())
        .fetch_all(&mut *tx)
        .await
        .map_err(unavailable)?;
    let (mut to, mut cc) = (Vec::new(), Vec::new());
    for recipient in &recipients {
        let address = AgentId::new(recipient.get("address"));
        match recipient.get::<String, _>("kind").as_str() {
            "DIRECT" => to.push(address),
            _ => cc.push(address),
        }
    }
    let listed = sqlx::query(
        "SELECT attachment_id, name FROM mail_attachments WHERE mail_id = $1 AND listed",
    )
    .bind(mail_id.as_uuid())
    .fetch_all(&mut *tx)
    .await
    .map_err(unavailable)?;
    let content = MailContent {
        mail_id,
        from: AgentId::new(row.get("sender_address")),
        author: MailAuthor::new(row.get::<String, _>("author")).map_err(corrupt)?,
        source_conversation: SourceConversation::new(row.get("source_conversation_id"))
            .map_err(corrupt)?,
        subject: row.get("subject"),
        body: row.get("body"),
        to,
        cc,
        attachments: listed
            .iter()
            .map(|attachment| AttachmentRef {
                attachment_id: AttachmentId::new(attachment.get("attachment_id")),
                name: attachment.get("name"),
            })
            .collect(),
    }
    .canonical();
    Ok(Some(MailView {
        content,
        created_at: row.get("created_at"),
    }))
}

fn corrupt(_: bc_jobs::domain::mail::refusal::MailRefusal) -> PortError {
    PortError::StoredStateRejected(bc_jobs::JobsError::CorruptState {
        reason_code: "stored_mail_unreadable",
    })
}

pub async fn standing_of(
    tx: &mut PgConnection,
    addresses: &[Uuid],
) -> Result<HashMap<Uuid, AddressStanding>, PortError> {
    let sql = format!(
        "SELECT {}, d.initial_job_id::uuid AS initial_job_id {} WHERE d.agent_id = ANY($1)",
        entry_columns(),
        ENTRY_TABLES
    );
    let held = sqlx::query(&sql)
        .bind(addresses)
        .fetch_all(&mut *tx)
        .await
        .map_err(unavailable)?;
    let removed: Vec<Uuid> = sqlx::query_scalar(
        "SELECT agent_id FROM directory_removed_addresses WHERE agent_id = ANY($1)",
    )
    .bind(addresses)
    .fetch_all(&mut *tx)
    .await
    .map_err(unavailable)?;
    let mut standings: HashMap<Uuid, AddressStanding> = removed
        .into_iter()
        .map(|address| (address, AddressStanding::Removed))
        .collect();
    for row in &held {
        let (entry, obstacles) = entry_of(row)?;
        standings.insert(
            entry.agent_id.as_uuid(),
            AddressStanding::Held(HeldAddress {
                entry,
                initial_job_id: JobId::new(row.get("initial_job_id"))?,
                obstacles,
            }),
        );
    }
    Ok(standings)
}
