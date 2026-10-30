use async_trait::async_trait;
use bc_jobs::domain::ids::{ResultId, RunId};
use bc_jobs::domain::payload::CallMode;
use bc_jobs::domain::requester::Requester;
use bc_jobs::domain::runner::answer::{
    ResultRefusal, guard_keeps_a_result, guard_reads_result, guard_result_alive, result_expires_at,
};
use bc_jobs::domain::runner::refusal::RunnerRefusal;
use bc_jobs::domain::runner::views::StoredResult;
use bc_jobs::domain::runner::work::WorkNamed;
use bc_jobs::ports::PortError;
use bc_jobs::ports::environment::IdFactory;
use bc_jobs::ports::result::Results;
use chrono::{DateTime, Utc};
use sqlx::Row;

use super::PgStore;
use super::acts_for::acts_for_in;
use super::hydrate::unavailable;
use super::work::require_work;

fn corrupt(error: serde_json::Error) -> PortError {
    PortError::Unavailable {
        detail: error.to_string(),
    }
}

#[async_trait]
impl Results for PgStore {
    async fn hand_over_result(
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
                 kept_at, expires_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(result_id.as_uuid())
        .bind(context.job_id.as_uuid())
        .bind(context.run_id.as_uuid())
        .bind(addressed.as_uuid())
        .bind(&answer.message)
        .bind(serde_json::to_value(&answer.attachments).map_err(corrupt)?)
        .bind(at)
        .bind(result_expires_at(at, None))
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
        tx.commit().await.map_err(unavailable)?;
        Ok(Ok(result_id))
    }

    async fn read_result(
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
            "SELECT addressed_run_id::uuid AS addressed, message, attachments, expires_at \
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
        if let Err(refusal) = guard_result_alive(row.get("expires_at"), at) {
            return Ok(Err(refusal.into()));
        }
        let answer = StoredResult {
            message: row.get("message"),
            attachments: serde_json::from_value(row.get("attachments")).map_err(corrupt)?,
        };
        if keep {
            sqlx::query(
                "UPDATE job_results SET read_at = $2, expires_at = $3 WHERE result_id = $1",
            )
            .bind(result_id.as_uuid())
            .bind(at)
            .bind(result_expires_at(at, Some(at)))
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
        let purged = sqlx::query("DELETE FROM job_results WHERE expires_at <= $1")
            .bind(now)
            .execute(self.pool())
            .await
            .map_err(unavailable)?;
        Ok(purged.rows_affected())
    }
}
