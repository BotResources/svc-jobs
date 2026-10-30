use std::collections::HashMap;

use bc_jobs::domain::directory::AgentState;
use bc_jobs::domain::directory::listing::{DirectoryEntry, RestartObstacles};
use bc_jobs::domain::ids::{AgentId, AttachmentId, JobId, MailId, RunId};
use bc_jobs::domain::keys::RunnerTypeKey;
use bc_jobs::domain::mail::author::{MailAuthor, SourceConversation};
use bc_jobs::domain::mail::content::{AttachmentRef, MailContent};
use bc_jobs::domain::mail::delivery::{AddressStanding, HeldAddress};
use bc_jobs::ports::PortError;
use chrono::{DateTime, Utc};
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use crate::db::directory::PARENT_TERMINAL;
use crate::db::hydrate::unavailable;

#[derive(Debug, Clone)]
pub struct MailView {
    pub content: MailContent,
    pub created_at: DateTime<Utc>,
}

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
    let held = sqlx::query(
&        "SELECT d.agent_id, d.run_id::uuid AS run_id, d.initial_job_id::uuid AS initial_job_id, d.state, d.has_inbox, d.declares_restart, \
            EXISTS (SELECT 1 FROM saved_state_agents s WHERE s.agent_id = d.agent_id) AS saved, \
            rt.type_key, \
            coalesce(reg.lifecycle = 'RETIRED', false) AS retired, \
            {PARENT_TERMINAL} AS parent_terminal \
         FROM directory_agents d \
         JOIN runs r ON r.id = d.run_id \
         JOIN jobs j ON j.id = r.job_id \
         JOIN runner_types rt ON rt.id = j.runner_type_id \
         LEFT JOIN registered_runner_types reg ON reg.runner_type_id = rt.id \
         WHERE d.agent_id = ANY($1)".replace("{PARENT_TERMINAL}", PARENT_TERMINAL))
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
        let address: Uuid = row.get("agent_id");
        standings.insert(
            address,
            AddressStanding::Held(HeldAddress {
                entry: DirectoryEntry {
                    agent_id: AgentId::new(address),
                    run_id: RunId::new(row.get("run_id"))?,
                    runner_type: RunnerTypeKey::new(row.get::<String, _>("type_key"))?,
                    state: AgentState::from_db_str(&row.get::<String, _>("state"))?,
                    has_inbox: row.get("has_inbox"),
                    declares_restart: row.get("declares_restart"),
                    holds_saved_state: row.get("saved"),
                },
                initial_job_id: JobId::new(row.get("initial_job_id"))?,
                obstacles: RestartObstacles {
                    runner_type_retired: row.get("retired"),
                    parent_job_terminal: row.get("parent_terminal"),
                },
            }),
        );
    }
    Ok(standings)
}
