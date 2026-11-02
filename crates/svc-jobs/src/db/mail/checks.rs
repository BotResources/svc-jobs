use bc_jobs::domain::ids::{AgentId, AttachmentId, RunId};
use bc_jobs::domain::mail::access::guard_sender_holds;
use bc_jobs::domain::mail::content::{MailContent, guard_attachments_uploaded};
use bc_jobs::domain::mail::delivery::{AddressStanding, Recipient, RecipientKind};
use bc_jobs::domain::mail::refusal::MailRefusal;
use bc_jobs::ports::PortError;
use sqlx::PgConnection;
use uuid::Uuid;

use super::load::standing_of;
use crate::db::hydrate::unavailable;

pub(super) async fn guard_sender(
    tx: &mut PgConnection,
    from: AgentId,
    run_id: RunId,
) -> Result<Result<(), MailRefusal>, PortError> {
    let holder: Option<Uuid> = sqlx::query_scalar(
        "SELECT run_id::uuid FROM directory_agents WHERE agent_id = $1 AND has_inbox",
    )
    .bind(from.as_uuid())
    .fetch_optional(&mut *tx)
    .await
    .map_err(unavailable)?;
    let holder = holder.map(RunId::new).transpose()?;
    Ok(guard_sender_holds(holder, run_id))
}

pub(super) async fn guard_attachments(
    tx: &mut PgConnection,
    content: &MailContent,
) -> Result<Result<(), MailRefusal>, PortError> {
    let named: Vec<Uuid> = content
        .attachments
        .iter()
        .map(|attachment| attachment.attachment_id.as_uuid())
        .collect();
    let uploaded: Vec<Uuid> = sqlx::query_scalar(
        "SELECT attachment_id FROM mail_attachments \
         WHERE mail_id = $1 AND uploader_address = $2 AND attachment_id = ANY($3)",
    )
    .bind(content.mail_id.as_uuid())
    .bind(content.from.as_uuid())
    .bind(&named)
    .fetch_all(&mut *tx)
    .await
    .map_err(unavailable)?;
    let uploaded: Vec<AttachmentId> = uploaded.into_iter().map(AttachmentId::new).collect();
    Ok(guard_attachments_uploaded(&uploaded, content))
}

pub(super) async fn recipients_of(
    tx: &mut PgConnection,
    content: &MailContent,
) -> Result<Vec<Recipient>, PortError> {
    let addresses: Vec<Uuid> = content
        .to
        .iter()
        .chain(&content.cc)
        .map(|address| address.as_uuid())
        .collect();
    let mut standings = standing_of(tx, &addresses).await?;
    let kinds = content
        .to
        .iter()
        .map(|address| (*address, RecipientKind::Direct))
        .chain(
            content
                .cc
                .iter()
                .map(|address| (*address, RecipientKind::Cc)),
        );
    Ok(kinds
        .map(|(address, kind)| Recipient {
            address,
            kind,
            standing: standings
                .remove(&address.as_uuid())
                .unwrap_or(AddressStanding::Unknown),
        })
        .collect())
}
