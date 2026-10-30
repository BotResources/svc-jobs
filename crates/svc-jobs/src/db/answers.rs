use bc_jobs::domain::ids::{ResultId, RunId};
use bc_jobs::domain::payload::{CallMode, PayloadAttachment};
use bc_jobs::domain::requester::Requester;
use bc_jobs::domain::runner::answer::{
    ResultRefusal, guard_keeps_a_result, guard_reads_result, guard_result_alive,
};
use bc_jobs::domain::runner::refusal::RunnerRefusal;
use bc_jobs::ports::PortError;
use bc_jobs::ports::environment::IdFactory;
use chrono::{DateTime, Utc};
use sqlx::Row;

use super::PgStore;
use super::acts_for::acts_for_in;
use super::hydrate::unavailable;
use super::work::{WorkNamed, require_work};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredResult {
    pub message: String,
    pub attachments: Vec<PayloadAttachment>,
}

fn corrupt(error: serde_json::Error) -> PortError {
    PortError::Unavailable {
        detail: error.to_string(),
    }
}

impl PgStore {
    pub async fn hand_over_result(
        &self,
        work: WorkNamed<'_>,
        answer: &StoredResult,
        ids: &dyn IdFactory,
        at: DateTime<Utc>,
    ) -> Result<Result<ResultId, RunnerRefusal>, PortError> {
        let mut tx = self.begin().await?;
        let context = match require_work(&mut tx, work, false).await? {
            Ok(context) => context,
            Err(refusal) => return Ok(Err(refusal.into())),
        };
        let row = sqlx::query("SELECT requester, call FROM jobs WHERE id = $1")
            .bind(context.job_id.as_uuid())
            .fetch_one(&mut *tx)
            .await
            .map_err(unavailable)?;
        let mode = match row.get::<Option<serde_json::Value>, _>("call") {
            Some(call) => Some(
                serde_json::from_value::<bc_jobs::domain::job::JobCall>(call)
                    .map_err(corrupt)?
                    .mode,
            ),
            None => None,
        };
        let requester: Option<Requester> = row
            .get::<Option<serde_json::Value>, _>("requester")
            .map(serde_json::from_value)
            .transpose()
            .map_err(corrupt)?;
        if let Err(refusal) = guard_keeps_a_result(mode) {
            return Ok(Err(refusal.into()));
        }
        let (Some(CallMode::Sync), Some(Requester::Run(addressed))) = (mode, requester) else {
            return Ok(Err(ResultRefusal::NotSyncCall.into()));
        };
        let result_id = ResultId::new(ids.next())?;
        sqlx::query(
            "INSERT INTO job_results \
                (result_id, job_id, produced_by_run_id, addressed_run_id, message, attachments, \
                 kept_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(result_id.as_uuid())
        .bind(context.job_id.as_uuid())
        .bind(context.run_id.as_uuid())
        .bind(addressed.as_uuid())
        .bind(&answer.message)
        .bind(serde_json::to_value(&answer.attachments).map_err(corrupt)?)
        .bind(at)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
        tx.commit().await.map_err(unavailable)?;
        Ok(Ok(result_id))
    }

    pub async fn read_result(
        &self,
        work: WorkNamed<'_>,
        result_id: ResultId,
        keep: bool,
        at: DateTime<Utc>,
    ) -> Result<Result<StoredResult, RunnerRefusal>, PortError> {
        let mut tx = self.begin().await?;
        let context = match require_work(&mut tx, work, false).await? {
            Ok(context) => context,
            Err(refusal) => return Ok(Err(refusal.into())),
        };
        let Some(row) = sqlx::query(
            "SELECT addressed_run_id::uuid AS addressed, message, attachments, kept_at, read_at \
             FROM job_results WHERE result_id = $1 FOR UPDATE",
        )
        .bind(result_id.as_uuid())
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?
        else {
            return Ok(Err(ResultRefusal::ResultUnknown.into()));
        };
        let addressed = RunId::new(row.get("addressed"))?;
        let acts_for = acts_for_in(&mut tx, context.run_id).await?;
        if let Err(refusal) = guard_reads_result(addressed, context.run_id, &acts_for) {
            return Ok(Err(refusal.into()));
        }
        if let Err(refusal) = guard_result_alive(row.get("kept_at"), row.get("read_at"), at) {
            return Ok(Err(refusal.into()));
        }
        let answer = StoredResult {
            message: row.get("message"),
            attachments: serde_json::from_value(row.get("attachments")).map_err(corrupt)?,
        };
        if keep {
            sqlx::query("UPDATE job_results SET read_at = $2 WHERE result_id = $1")
                .bind(result_id.as_uuid())
                .bind(at)
                .execute(&mut *tx)
                .await
                .map_err(unavailable)?;
        } else {
            sqlx::query("DELETE FROM job_results WHERE result_id = $1")
                .bind(result_id.as_uuid())
                .execute(&mut *tx)
                .await
                .map_err(unavailable)?;
        }
        tx.commit().await.map_err(unavailable)?;
        Ok(Ok(answer))
    }
}

impl PgStore {
    pub async fn purge_expired_results(&self, now: DateTime<Utc>) -> Result<u64, PortError> {
        let retention = bc_jobs::domain::runner::answer::RESULT_RETENTION.num_seconds();
        let purged = sqlx::query(
            "DELETE FROM job_results \
             WHERE coalesce(read_at, kept_at) + make_interval(secs => $2::bigint) <= $1",
        )
        .bind(now)
        .bind(retention)
        .execute(self.pool())
        .await
        .map_err(unavailable)?;
        Ok(purged.rows_affected())
    }
}
