use bc_jobs::domain::ids::ResolutionId;
use bc_jobs::domain::job::resolution::JobFailureCause;
use bc_jobs::event::job_facts::{
    JobCancelled, JobCompleted, JobDeleted, JobFailed, JobQueued, ManualRetryStarted,
};
use bc_jobs::ports::PortError;
use chrono::{DateTime, Utc};
use sqlx::PgConnection;
use uuid::Uuid;

use super::refs;
use crate::db::hydrate::unavailable;

pub async fn queued(
    tx: &mut PgConnection,
    fact: &JobQueued,
    at: DateTime<Utc>,
) -> Result<(), PortError> {
    let runner_type = refs::runner_type_id(tx, &fact.runner_type).await?;
    let producer = refs::producer_id(tx, &fact.producer).await?;
    let source = match &fact.source {
        Some(source) => {
            Some(refs::source_entity_id(tx, producer, source.entity_id().as_uuid()).await?)
        }
        None => None,
    };
    let triggered_by = match &fact.triggered_by {
        Some(user) => Some(refs::observe_known_user(tx, user, at).await?),
        None => None,
    };
    sqlx::query(
        "INSERT INTO jobs (id, runner_type_id, config, parent_job_id, predecessor_job_id, \
         triggered_by_id, producer_id, source_entity_id, max_attempts, created_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
    )
    .bind(fact.job_id.as_uuid())
    .bind(runner_type)
    .bind(fact.config.as_ref().map(|config| config.as_value().clone()))
    .bind(fact.parent_job_id.map(|id| id.as_uuid()))
    .bind(fact.predecessor_job_id.map(|id| id.as_uuid()))
    .bind(triggered_by)
    .bind(source.is_none().then_some(producer))
    .bind(source)
    .bind(
        fact.max_attempts
            .map(|budget| i32::try_from(budget.get()).unwrap_or(i32::MAX)),
    )
    .bind(at)
    .execute(&mut *tx)
    .await
    .map_err(unavailable)?;
    Ok(())
}

pub async fn completed(
    tx: &mut PgConnection,
    fact: &JobCompleted,
    at: DateTime<Utc>,
) -> Result<(), PortError> {
    resolution(
        tx,
        fact.resolution_id,
        fact.job_id.as_uuid(),
        "COMPLETED",
        at,
        None,
        None,
    )
    .await
}

pub async fn cancelled(
    tx: &mut PgConnection,
    fact: &JobCancelled,
    at: DateTime<Utc>,
) -> Result<(), PortError> {
    resolution(
        tx,
        fact.resolution_id,
        fact.job_id.as_uuid(),
        "CANCELLED",
        at,
        None,
        None,
    )
    .await
}

pub async fn failed(
    tx: &mut PgConnection,
    fact: &JobFailed,
    at: DateTime<Utc>,
) -> Result<(), PortError> {
    resolution(
        tx,
        fact.resolution_id,
        fact.job_id.as_uuid(),
        "FAILED",
        at,
        Some(fact.failure_cause),
        fact.caused_by_run_id.map(|run| run.as_uuid()),
    )
    .await
}

async fn resolution(
    tx: &mut PgConnection,
    id: ResolutionId,
    job_id: Uuid,
    kind: &str,
    at: DateTime<Utc>,
    cause: Option<JobFailureCause>,
    caused_by_run_id: Option<Uuid>,
) -> Result<(), PortError> {
    let existing: Option<Uuid> =
        sqlx::query_scalar("SELECT id::uuid FROM job_resolutions WHERE job_id = $1 LIMIT 1")
            .bind(job_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(unavailable)?;
    if existing.is_some() {
        return Err(PortError::ConcurrentModification);
    }
    sqlx::query(
        "INSERT INTO job_resolutions (id, job_id, kind, occurred_at, failure_cause, \
         caused_by_run_id) VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(id.as_uuid())
    .bind(job_id)
    .bind(kind)
    .bind(at)
    .bind(cause.map(|cause| cause.as_db_str()))
    .bind(caused_by_run_id)
    .execute(&mut *tx)
    .await
    .map_err(unavailable)?;
    Ok(())
}

pub async fn manual_retry_started(
    tx: &mut PgConnection,
    fact: &ManualRetryStarted,
    at: DateTime<Utc>,
) -> Result<(), PortError> {
    let requested_by = refs::observe_known_user(tx, &fact.requested_by, at).await?;
    sqlx::query(
        "INSERT INTO manual_retries (id, failed_resolution_id, predecessor_job_id, \
         successor_job_id, requested_by_id, requested_at) VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(fact.manual_retry_id.as_uuid())
    .bind(fact.failed_resolution_id.as_uuid())
    .bind(fact.job_id.as_uuid())
    .bind(fact.successor_job_id.as_uuid())
    .bind(requested_by)
    .bind(at)
    .execute(&mut *tx)
    .await
    .map_err(unavailable)?;
    Ok(())
}

pub async fn deleted(
    tx: &mut PgConnection,
    fact: &JobDeleted,
    at: DateTime<Utc>,
) -> Result<(), PortError> {
    let deleted_by = refs::observe_known_user(tx, &fact.deleted_by, at).await?;
    sqlx::query(
        "INSERT INTO job_deletions (job_id, deleted_by_id, deleted_at) VALUES ($1, $2, $3)",
    )
    .bind(fact.job_id.as_uuid())
    .bind(deleted_by)
    .bind(at)
    .execute(&mut *tx)
    .await
    .map_err(unavailable)?;
    Ok(())
}
