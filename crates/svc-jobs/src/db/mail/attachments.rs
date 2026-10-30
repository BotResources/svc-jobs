use async_trait::async_trait;
use bc_jobs::domain::ids::{AgentId, AttachmentId, MailId};
use bc_jobs::domain::mail::content::{
    AttachmentAdmission, AttachmentBody, StoredAttachment, decide_attachment_upload,
};
use bc_jobs::domain::mail::refusal::MailRefusal;
use bc_jobs::domain::mail::views::AttachmentMeta;
use bc_jobs::domain::runner::refusal::RunnerRefusal;
use bc_jobs::domain::runner::work::WorkNamed;
use bc_jobs::ports::PortError;
use bc_jobs::ports::mail::{AttachmentStore, UploadAttachment};
use chrono::{DateTime, Utc};
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use super::send::guard_sender;
use crate::db::PgStore;
use crate::db::hydrate::unavailable;
use crate::db::work::{addresses_of_run, require_work};

pub async fn listed_attachments(
    tx: &mut PgConnection,
    mail_id: MailId,
) -> Result<Vec<AttachmentMeta>, PortError> {
    let rows = sqlx::query(
        "SELECT attachment_id, name, content_type, size FROM mail_attachments \
         WHERE mail_id = $1 AND listed ORDER BY attachment_id",
    )
    .bind(mail_id.as_uuid())
    .fetch_all(&mut *tx)
    .await
    .map_err(unavailable)?;
    Ok(rows
        .iter()
        .map(|row| AttachmentMeta {
            attachment_id: AttachmentId::new(row.get("attachment_id")),
            name: row.get("name"),
            content_type: row.get("content_type"),
            size: row.get("size"),
        })
        .collect())
}

#[async_trait]
impl AttachmentStore for PgStore {
    async fn upload_attachment(
        &self,
        request: UploadAttachment<'_>,
        at: DateTime<Utc>,
    ) -> Result<Result<(), RunnerRefusal>, PortError> {
        let mut tx = self.begin().await?;
        let context = match require_work(&mut tx, request.work, true).await? {
            Ok(context) => context,
            Err(refusal) => return Ok(Err(RunnerRefusal::Work(refusal))),
        };
        if let Err(refusal) = guard_sender(&mut tx, request.from, context.run_id).await? {
            return Ok(Err(refusal.into()));
        }
        let incoming = AttachmentBody {
            name: request.name.clone(),
            content_type: request.content_type.clone(),
            content: request.content.clone(),
        };
        let inserted = sqlx::query(
            "INSERT INTO mail_attachments \
                (mail_id, attachment_id, uploader_address, name, content_type, content, size, \
                 uploaded_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8) \
             ON CONFLICT (mail_id, attachment_id) DO NOTHING",
        )
        .bind(request.mail_id.as_uuid())
        .bind(request.attachment_id.as_uuid())
        .bind(request.from.as_uuid())
        .bind(&incoming.name)
        .bind(&incoming.content_type)
        .bind(&incoming.content)
        .bind(i64::try_from(incoming.content.len()).unwrap_or(i64::MAX))
        .bind(at)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?
        .rows_affected();
        if inserted == 0 {
            let stored = locked_attachment(&mut tx, request.mail_id, request.attachment_id).await?;
            match decide_attachment_upload(stored.as_ref(), request.from, &incoming) {
                AttachmentAdmission::Absorb => return Ok(Ok(())),
                AttachmentAdmission::Refuse(refusal) => return Ok(Err(refusal.into())),
                AttachmentAdmission::Store => {
                    replace_attachment(&mut tx, &request, &incoming, at).await?;
                }
            }
        }
        tx.commit().await.map_err(unavailable)?;
        Ok(Ok(()))
    }

    async fn fetch_attachment(
        &self,
        work: WorkNamed<'_>,
        mail_id: MailId,
        attachment_id: AttachmentId,
    ) -> Result<Result<AttachmentBody, RunnerRefusal>, PortError> {
        let mut tx = self.begin().await?;
        let context = match require_work(&mut tx, work, false).await? {
            Ok(context) => context,
            Err(refusal) => return Ok(Err(RunnerRefusal::Work(refusal))),
        };
        let addresses: Vec<Uuid> = addresses_of_run(&mut tx, context.run_id)
            .await?
            .into_iter()
            .map(|address| address.as_uuid())
            .collect();
        let row = sqlx::query(
            "SELECT a.name, a.content_type, a.content FROM mail_attachments a \
             JOIN mail_recipients r ON r.mail_id = a.mail_id \
             WHERE a.mail_id = $1 AND a.attachment_id = $2 AND a.listed \
               AND r.address = ANY($3) LIMIT 1",
        )
        .bind(mail_id.as_uuid())
        .bind(attachment_id.as_uuid())
        .bind(&addresses)
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?;
        Ok(match row {
            Some(row) => Ok(AttachmentBody {
                name: row.get("name"),
                content_type: row.get("content_type"),
                content: row.get("content"),
            }),
            None => Err(MailRefusal::MailNotYours.into()),
        })
    }
}

async fn locked_attachment(
    tx: &mut PgConnection,
    mail_id: MailId,
    attachment_id: AttachmentId,
) -> Result<Option<StoredAttachment>, PortError> {
    let row = sqlx::query(
        "SELECT uploader_address, name, content_type, content, \
            EXISTS (SELECT 1 FROM mails WHERE mail_id = $1) AS sent \
         FROM mail_attachments WHERE mail_id = $1 AND attachment_id = $2 FOR UPDATE",
    )
    .bind(mail_id.as_uuid())
    .bind(attachment_id.as_uuid())
    .fetch_optional(&mut *tx)
    .await
    .map_err(unavailable)?;
    Ok(row.map(|row| StoredAttachment {
        uploader: AgentId::new(row.get("uploader_address")),
        body: AttachmentBody {
            name: row.get("name"),
            content_type: row.get("content_type"),
            content: row.get("content"),
        },
        mail_sent: row.get("sent"),
    }))
}

async fn replace_attachment(
    tx: &mut PgConnection,
    request: &UploadAttachment<'_>,
    body: &AttachmentBody,
    at: DateTime<Utc>,
) -> Result<(), PortError> {
    sqlx::query(
        "UPDATE mail_attachments SET name = $4, content_type = $5, content = $6, size = $7, \
            uploaded_at = $8 \
         WHERE mail_id = $1 AND attachment_id = $2 AND uploader_address = $3",
    )
    .bind(request.mail_id.as_uuid())
    .bind(request.attachment_id.as_uuid())
    .bind(request.from.as_uuid())
    .bind(&body.name)
    .bind(&body.content_type)
    .bind(&body.content)
    .bind(i64::try_from(body.content.len()).unwrap_or(i64::MAX))
    .bind(at)
    .execute(&mut *tx)
    .await
    .map_err(unavailable)?;
    Ok(())
}
