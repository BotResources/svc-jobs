use bc_jobs::domain::ownership::ActorRef;
use bc_jobs::domain::payload::{JobPayload, PayloadAttachment};
use bc_jobs::domain::requester::Requester;
use bc_jobs::domain::runner::kept_payload::{
    ExistingJob, KeptPayload, StoredPayload, UploadAdmission, decide_upload,
    guard_job_serves_kept_payload,
};
use bc_jobs::domain::runner::refusal::RunnerRefusal;
use bc_jobs::ports::PortError;
use chrono::{DateTime, Utc};
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use super::PgStore;
use super::hydrate::unavailable;
use super::work::{WorkNamed, require_work};

const PAYLOAD_SOURCE_SQL: &str = "WITH RECURSIVE src(job_id, depth) AS ( \
        SELECT $1::uuid, 0 \
      UNION ALL \
        SELECT coalesce(j.predecessor_job_id, origin.job_id), s.depth + 1 FROM src s \
        JOIN jobs j ON j.id = s.job_id \
        LEFT JOIN restart_origins ro ON ro.job_id = j.id \
        LEFT JOIN runs origin ON origin.id = ro.restarts_run_id \
        WHERE s.depth < 64 \
          AND coalesce(j.predecessor_job_id, origin.job_id) IS NOT NULL) \
    SELECT kp.job_id FROM src s JOIN kept_payloads kp ON kp.job_id = s.job_id \
    ORDER BY s.depth LIMIT 1";

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
        let job = existing_job(&mut tx, for_job_id).await?;
        let known = stored(&mut tx, for_job_id).await?;
        let admission = match decide_upload(&uploader, &payload, job.as_ref(), known.as_ref()) {
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

    pub async fn stored_payload(&self, job_id: Uuid) -> Result<Option<StoredPayload>, PortError> {
        let mut connection = self.pool().acquire().await.map_err(unavailable)?;
        stored(&mut connection, job_id).await
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
        let payload = job_payload(&mut tx, context.job_id.as_uuid()).await?;
        if let Err(refusal) = guard_job_serves_kept_payload(payload.as_ref()) {
            return Ok(Err(refusal));
        }
        let source: Option<Uuid> = sqlx::query_scalar(PAYLOAD_SOURCE_SQL)
            .bind(context.job_id.as_uuid())
            .fetch_optional(&mut *tx)
            .await
            .map_err(unavailable)?;
        let found = match source {
            Some(source) => stored(&mut tx, source).await?,
            None => None,
        };
        Ok(found
            .map(|found| found.payload)
            .ok_or(RunnerRefusal::NoKeptPayload))
    }
}

async fn existing_job(
    tx: &mut PgConnection,
    job_id: Uuid,
) -> Result<Option<ExistingJob>, PortError> {
    let row = sqlx::query("SELECT requester, payload FROM jobs WHERE id = $1")
        .bind(job_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?;
    let Some(row) = row else {
        return Ok(None);
    };
    let requester = match row.get::<Option<serde_json::Value>, _>("requester") {
        Some(stored) => serde_json::from_value(stored).map_err(corrupt)?,
        None => Requester::Service(None),
    };
    let payload = match row.get::<Option<serde_json::Value>, _>("payload") {
        Some(stored) => Some(serde_json::from_value(stored).map_err(corrupt)?),
        None => None,
    };
    Ok(Some(ExistingJob { requester, payload }))
}

async fn job_payload(tx: &mut PgConnection, job_id: Uuid) -> Result<Option<JobPayload>, PortError> {
    let stored: Option<serde_json::Value> =
        sqlx::query_scalar("SELECT payload FROM jobs WHERE id = $1")
            .bind(job_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(unavailable)?
            .flatten();
    stored
        .map(|stored| serde_json::from_value(stored).map_err(corrupt))
        .transpose()
}
