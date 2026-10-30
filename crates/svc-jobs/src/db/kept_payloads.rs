use bc_jobs::domain::ownership::ActorRef;
use bc_jobs::domain::payload::PayloadAttachment;
use bc_jobs::domain::requester::Requester;
use bc_jobs::domain::runner::kept_payload::{
    KeptPayload, StoredPayload, UploadAdmission, decide_upload,
};
use bc_jobs::domain::runner::refusal::RunnerRefusal;
use bc_jobs::ports::PortError;
use chrono::{DateTime, Utc};
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use super::PgStore;
use super::hydrate::unavailable;
use super::work::{WorkNamed, require_work};

pub enum Uploader<'a> {
    Run(WorkNamed<'a>),
    Service(ActorRef),
}

fn corrupt(error: serde_json::Error) -> PortError {
    PortError::Unavailable {
        detail: error.to_string(),
    }
}

async fn stored(tx: &mut PgConnection, job_id: Uuid) -> Result<Option<StoredPayload>, PortError> {
    let Some(row) =
        sqlx::query("SELECT uploader, message, attachments FROM kept_payloads WHERE job_id = $1")
            .bind(job_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(unavailable)?
    else {
        return Ok(None);
    };
    let attachments: Vec<PayloadAttachment> =
        serde_json::from_value(row.get("attachments")).map_err(corrupt)?;
    Ok(Some(StoredPayload {
        uploader: serde_json::from_value(row.get("uploader")).map_err(corrupt)?,
        payload: KeptPayload {
            message: row.get("message"),
            attachments,
        },
    }))
}

impl PgStore {
    pub async fn upload_payload(
        &self,
        uploader: Uploader<'_>,
        for_job_id: Uuid,
        payload: KeptPayload,
        at: DateTime<Utc>,
    ) -> Result<Result<UploadAdmission, RunnerRefusal>, PortError> {
        let mut tx = self.begin().await?;
        let uploader = match uploader {
            Uploader::Service(actor) => Requester::Service(Some(actor)),
            Uploader::Run(work) => match require_work(&mut tx, work, false).await? {
                Ok(context) => Requester::Run(context.run_id),
                Err(refusal) => return Ok(Err(refusal.into())),
            },
        };
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(format!("kept_payload:{for_job_id}"))
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
        let job_requester = job_requester(&mut tx, for_job_id).await?;
        let known = stored(&mut tx, for_job_id).await?;
        let admission =
            match decide_upload(&uploader, &payload, job_requester.as_ref(), known.as_ref()) {
                Ok(admission) => admission,
                Err(refusal) => return Ok(Err(refusal.into())),
            };
        if admission == UploadAdmission::Store {
            sqlx::query(
                "INSERT INTO kept_payloads (job_id, uploader, message, attachments, uploaded_at) \
                 VALUES ($1, $2, $3, $4, $5)",
            )
            .bind(for_job_id)
            .bind(serde_json::to_value(uploader).map_err(corrupt)?)
            .bind(&payload.message)
            .bind(serde_json::to_value(&payload.attachments).map_err(corrupt)?)
            .bind(at)
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
        }
        tx.commit().await.map_err(unavailable)?;
        Ok(Ok(admission))
    }

    pub async fn fetch_payload(
        &self,
        work: WorkNamed<'_>,
    ) -> Result<Result<KeptPayload, RunnerRefusal>, PortError> {
        let mut tx = self.begin().await?;
        let context = match require_work(&mut tx, work, false).await? {
            Ok(context) => context,
            Err(refusal) => return Ok(Err(refusal.into())),
        };
        let found = stored(&mut tx, context.job_id.as_uuid()).await?;
        Ok(found
            .map(|found| found.payload)
            .ok_or(RunnerRefusal::NoKeptPayload))
    }
}

async fn job_requester(
    tx: &mut PgConnection,
    job_id: Uuid,
) -> Result<Option<Requester>, PortError> {
    let row = sqlx::query("SELECT requester FROM jobs WHERE id = $1")
        .bind(job_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?;
    let Some(row) = row else {
        return Ok(None);
    };
    match row.get::<Option<serde_json::Value>, _>("requester") {
        Some(stored) => serde_json::from_value(stored).map(Some).map_err(corrupt),
        None => Ok(Some(Requester::Service(None))),
    }
}
