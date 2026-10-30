use bc_jobs::domain::ids::{AgentId, AttachmentId, MailId};
use bc_jobs::domain::mail::refusal::MailRefusal;
use bc_jobs::domain::runner::refusal::RunnerRefusal;
use bc_jobs::ports::PortError;
use chrono::{DateTime, Utc};
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use super::send::guard_sender;
use crate::db::PgStore;
use crate::db::hydrate::unavailable;
use crate::db::work::{WorkNamed, addresses_of_run, require_work};

pub struct UploadAttachment<'a> {
    pub work: WorkNamed<'a>,
    pub mail_id: MailId,
    pub attachment_id: AttachmentId,
    pub from: AgentId,
    pub name: String,
    pub content_type: String,
    pub content: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttachmentMeta {
    pub attachment_id: AttachmentId,
    pub name: String,
    pub content_type: String,
    pub size: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttachmentContent {
    pub name: String,
    pub content_type: String,
    pub content: String,
}

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

impl PgStore {
    pub async fn upload_attachment(
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
        let existing = sqlx::query(
            "SELECT uploader_address, name, content_type, content, \
                EXISTS (SELECT 1 FROM mails WHERE mail_id = $1) AS sent \
             FROM mail_attachments WHERE mail_id = $1 AND attachment_id = $2",
        )
        .bind(request.mail_id.as_uuid())
        .bind(request.attachment_id.as_uuid())
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?;
        if let Some(row) = existing {
            let same_uploader = row.get::<Uuid, _>("uploader_address") == request.from.as_uuid();
            let identical = row.get::<String, _>("name") == request.name
                && row.get::<String, _>("content_type") == request.content_type
                && row.get::<String, _>("content") == request.content;
            if same_uploader && identical {
                return Ok(Ok(()));
            }
            if !same_uploader || row.get::<bool, _>("sent") {
                return Ok(Err(MailRefusal::MailIdReused.into()));
            }
        }
        sqlx::query(
            "INSERT INTO mail_attachments \
                (mail_id, attachment_id, uploader_address, name, content_type, content, size, \
                 uploaded_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8) \
             ON CONFLICT (mail_id, attachment_id) DO UPDATE SET name = EXCLUDED.name, \
                content_type = EXCLUDED.content_type, content = EXCLUDED.content, \
                size = EXCLUDED.size, uploaded_at = EXCLUDED.uploaded_at",
        )
        .bind(request.mail_id.as_uuid())
        .bind(request.attachment_id.as_uuid())
        .bind(request.from.as_uuid())
        .bind(&request.name)
        .bind(&request.content_type)
        .bind(&request.content)
        .bind(i64::try_from(request.content.len()).unwrap_or(i64::MAX))
        .bind(at)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
        tx.commit().await.map_err(unavailable)?;
        Ok(Ok(()))
    }

    pub async fn fetch_attachment(
        &self,
        work: WorkNamed<'_>,
        mail_id: MailId,
        attachment_id: AttachmentId,
    ) -> Result<Result<AttachmentContent, RunnerRefusal>, PortError> {
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
            Some(row) => Ok(AttachmentContent {
                name: row.get("name"),
                content_type: row.get("content_type"),
                content: row.get("content"),
            }),
            None => Err(MailRefusal::MailNotYours.into()),
        })
    }
}
