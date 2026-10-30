use bc_jobs::domain::ids::ResolutionId;
use bc_jobs::domain::job::resolution::JobFailureCause;
use bc_jobs::event::job_facts::{
    JobCancelled, JobCompleted, JobDeleted, JobFailed, JobPriorityForced, JobPriorityRecalculated,
    JobQueued, ManualRetryStarted,
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
    let sql = format!(
        "INSERT INTO jobs (id, runner_type_id, config, parent_job_id, predecessor_job_id, \
         triggered_by_id, producer_id, source_entity_id, max_attempts, created_at, \
         requested_priority, effective_priority, forced_priority, root_job_id, \
         requester, passport, payload, call, parent_run_id) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, \
            coalesce($14, (SELECT {} FROM jobs p WHERE p.id = $4), $1), $15, $16, $17, $18, \
            coalesce( \
                (SELECT r.id FROM runs r WHERE r.id = ($15::jsonb ->> 'Run')::uuid), \
                (SELECT cr.run_id FROM current_runs cr WHERE cr.job_id = $4 LIMIT 1)))",
        crate::db::lineage::root_of("p"),
    );
    sqlx::query(&sql)
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
        .bind(fact.requested_priority.get_i16())
        .bind(fact.effective_priority.get_i16())
        .bind(fact.forced_priority.map(|level| level.get_i16()))
        .bind(fact.initial_job_id.map(|id| id.as_uuid()))
        .bind(as_json(&fact.requester)?)
        .bind(as_json(&fact.passport)?)
        .bind(as_json(&fact.payload)?)
        .bind(as_json(&fact.call)?)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
    super::waiting::begin_waiting(tx, fact.job_id.as_uuid(), None).await
}

fn as_json<T: serde::Serialize>(value: &Option<T>) -> Result<Option<serde_json::Value>, PortError> {
    value
        .as_ref()
        .map(|value| {
            serde_json::to_value(value).map_err(|_| {
                PortError::from(bc_jobs::JobsError::CorruptState {
                    reason_code: "job_fact_not_serializable",
                })
            })
        })
        .transpose()
}

pub async fn priority_forced(
    tx: &mut PgConnection,
    fact: &JobPriorityForced,
) -> Result<(), PortError> {
    sqlx::query(
        "UPDATE jobs SET forced_priority = $2, effective_priority = $2, \
         requested_priority = coalesce($3, requested_priority) WHERE id = $1",
    )
    .bind(fact.job_id.as_uuid())
    .bind(fact.priority.get_i16())
    .bind(fact.requested_priority.map(|level| level.get_i16()))
    .execute(&mut *tx)
    .await
    .map_err(unavailable)?;
    super::waiting::reprioritize(tx, fact.job_id.as_uuid(), fact.priority.get_i16()).await
}

pub async fn priority_recalculated(
    tx: &mut PgConnection,
    fact: &JobPriorityRecalculated,
) -> Result<(), PortError> {
    let updated = sqlx::query(
        "UPDATE jobs SET effective_priority = $2, \
         requested_priority = coalesce($3, requested_priority) \
         WHERE id = $1 AND forced_priority IS NULL",
    )
    .bind(fact.job_id.as_uuid())
    .bind(fact.effective_priority.get_i16())
    .bind(fact.requested_priority.map(|level| level.get_i16()))
    .execute(&mut *tx)
    .await
    .map_err(unavailable)?;
    if updated.rows_affected() != 1 {
        return Err(PortError::ConcurrentModification);
    }
    super::waiting::reprioritize(tx, fact.job_id.as_uuid(), fact.effective_priority.get_i16()).await
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
    super::waiting::end_waiting(tx, job_id).await
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
