use bc_jobs::domain::ids::{AgentId, ItemId, JobId, RunId};
use bc_jobs::domain::mail::content::{MailContent, SendAdmission};
use bc_jobs::domain::mail::delivery::{
    AddressStanding, Deliveries, Recipient, RecipientKind, decide_deliveries,
};
use bc_jobs::domain::mail::inbox::ItemKind;
use bc_jobs::domain::mail::refusal::MailRefusal;
use bc_jobs::domain::runner::refusal::RunnerRefusal;
use bc_jobs::ports::PortError;
use bc_jobs::ports::environment::IdFactory;
use br_core_events::EventMetadata;
use chrono::{DateTime, Utc};
use sqlx::PgConnection;
use uuid::Uuid;

use super::load::{load_mail, standing_of};
use crate::db::PgStore;
use crate::db::doorbell::{Arrival, Ring, ring};
use crate::db::hydrate::unavailable;
use crate::db::work::{WorkNamed, require_work};

pub struct SendMail<'a> {
    pub work: WorkNamed<'a>,
    pub content: MailContent,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sent {
    pub refused_restarts: Vec<AgentId>,
}

pub struct Minting<'a> {
    pub ids: &'a dyn IdFactory,
    pub metadata: &'a EventMetadata,
    pub at: DateTime<Utc>,
}

impl PgStore {
    pub async fn send_mail(
        &self,
        request: SendMail<'_>,
        minting: Minting<'_>,
    ) -> Result<Result<Sent, RunnerRefusal>, PortError> {
        let mut tx = self.begin().await?;
        let context = match require_work(&mut tx, request.work, true).await? {
            Ok(context) => context,
            Err(refusal) => return Ok(Err(RunnerRefusal::Work(refusal))),
        };
        let content = request.content;
        if let Err(refusal) = guard_sender(&mut tx, content.from, context.run_id).await? {
            return Ok(Err(refusal.into()));
        }
        let known = load_mail(&mut tx, content.mail_id).await?;
        match MailContent::admit(known.as_ref().map(|view| &view.content), &content) {
            Ok(SendAdmission::Absorbed) => {
                return Ok(Ok(Sent {
                    refused_restarts: vec![],
                }));
            }
            Ok(SendAdmission::New) => {}
            Err(refusal) => return Ok(Err(refusal.into())),
        }
        if let Err(refusal) = guard_attachments(&mut tx, &content).await? {
            return Ok(Err(refusal.into()));
        }
        let recipients = recipients_of(&mut tx, &content).await?;
        let Deliveries(plan) = match decide_deliveries(context.initial_job_id, &recipients) {
            Ok(plan) => plan,
            Err(refusal) => return Ok(Err(refusal.into())),
        };
        insert_mail(
            &mut tx,
            &content,
            context.run_id,
            context.initial_job_id,
            minting.at,
        )
        .await?;
        for delivery in &plan {
            let item_id = ItemId::new(minting.ids.next())?;
            let kind = match delivery.kind {
                RecipientKind::Direct => ItemKind::Mail,
                RecipientKind::Cc => ItemKind::CcNotice,
            };
            insert_item(
                &mut tx,
                item_id,
                kind,
                delivery.address,
                content.mail_id.as_uuid(),
                minting.at,
            )
            .await?;
            if delivery.rings_live_run
                && let Some(run_id) = live_run_of(&mut tx, delivery.address).await?
            {
                ring(
                    &mut tx,
                    Arrival {
                        run_id,
                        address: Some(delivery.address.as_uuid()),
                        item_id,
                        kind,
                    },
                    Ring {
                        record_id: minting.ids.next(),
                        event_id: minting.ids.next(),
                        metadata: minting.metadata,
                        at: minting.at,
                    },
                )
                .await?;
            }
        }
        tx.commit().await.map_err(unavailable)?;
        Ok(Ok(Sent {
            refused_restarts: plan
                .iter()
                .filter_map(|delivery| delivery.refused_restart.map(|refused| refused.address))
                .collect(),
        }))
    }
}

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
    Ok(match holder {
        None => Err(MailRefusal::SenderUnknown),
        Some(holder) if holder == run_id.as_uuid() => Ok(()),
        Some(_) => Err(MailRefusal::SenderNotOfRun),
    })
}

async fn guard_attachments(
    tx: &mut PgConnection,
    content: &MailContent,
) -> Result<Result<(), MailRefusal>, PortError> {
    for attachment in &content.attachments {
        let uploaded: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM mail_attachments \
             WHERE mail_id = $1 AND attachment_id = $2 AND uploader_address = $3)",
        )
        .bind(content.mail_id.as_uuid())
        .bind(attachment.attachment_id.as_uuid())
        .bind(content.from.as_uuid())
        .fetch_one(&mut *tx)
        .await
        .map_err(unavailable)?;
        if !uploaded {
            return Ok(Err(MailRefusal::AttachmentNotUploaded {
                attachment_id: attachment.attachment_id,
            }));
        }
    }
    Ok(Ok(()))
}

async fn recipients_of(
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

async fn insert_mail(
    tx: &mut PgConnection,
    content: &MailContent,
    run_id: RunId,
    initial_job_id: JobId,
    at: DateTime<Utc>,
) -> Result<(), PortError> {
    sqlx::query(
        "INSERT INTO mails (mail_id, sender_address, sender_run_id, initial_job_id, author, \
            source_conversation_id, subject, body, created_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
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
    .map_err(unavailable)?;
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
    Ok(())
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

async fn live_run_of(tx: &mut PgConnection, address: AgentId) -> Result<Option<RunId>, PortError> {
    let found: Option<Uuid> = sqlx::query_scalar(
        "SELECT d.run_id::uuid FROM directory_agents d WHERE d.agent_id = $1 AND d.state = 'RUNNING'",
    )
    .bind(address.as_uuid())
    .fetch_optional(&mut *tx)
    .await
    .map_err(unavailable)?;
    found.map(RunId::new).transpose().map_err(PortError::from)
}
