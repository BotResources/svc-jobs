use bc_jobs::event::job_facts::{
    RetryScheduled, RunCancellationRequested, RunCancelled, RunCompleted, RunDispatched, RunFailed,
    RunPlanDeclared, RunStarted, RunStepStarted,
};
use bc_jobs::ports::PortError;
use chrono::{DateTime, Utc};
use sqlx::PgConnection;
use uuid::Uuid;

use super::refs;
use crate::db::hydrate::unavailable;

pub async fn dispatched(
    tx: &mut PgConnection,
    fact: &RunDispatched,
    at: DateTime<Utc>,
) -> Result<(), PortError> {
    sqlx::query(
        "INSERT INTO runs (id, job_id, attempt_number, dispatched_at, automatic_retry_schedule_id, \
         max_run_duration_seconds) VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(fact.run_id.as_uuid())
    .bind(fact.job_id.as_uuid())
    .bind(i32::try_from(fact.attempt_number.get()).unwrap_or(i32::MAX))
    .bind(at)
    .bind(fact.automatic_retry_schedule_id.map(|id| id.as_uuid()))
    .bind(fact.max_run_duration.map(|duration| duration.seconds()))
    .execute(&mut *tx)
    .await
    .map_err(unavailable)?;
    sqlx::query(
        "INSERT INTO current_runs (run_id, job_id, runner_type_id) \
         SELECT $1, j.id, j.runner_type_id FROM jobs j WHERE j.id = $2 \
         ON CONFLICT (run_id) DO NOTHING",
    )
    .bind(fact.run_id.as_uuid())
    .bind(fact.job_id.as_uuid())
    .execute(&mut *tx)
    .await
    .map_err(unavailable)?;
    super::waiting::end_waiting(tx, fact.job_id.as_uuid()).await?;
    super::waiting::served(tx, fact.job_id.as_uuid(), at).await?;
    if fact.claimed_by.is_none() {
        super::waiting::trigger_published(tx, fact.run_id.as_uuid(), fact.job_id.as_uuid(), at)
            .await?;
    }
    if let Some(claim) = &fact.claimed_by {
        let instance = existing_instance(tx, &fact.runner_type, &claim.instance_key).await?;
        sqlx::query(
            "INSERT INTO run_reservations (run_id, instance_id, claim_id, reserved_at) \
             VALUES ($1, $2, $3, $4)",
        )
        .bind(fact.run_id.as_uuid())
        .bind(instance)
        .bind(claim.claim_id.as_uuid())
        .bind(at)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
    }
    Ok(())
}

async fn existing_instance(
    tx: &mut PgConnection,
    runner_type: &bc_jobs::domain::keys::RunnerTypeKey,
    instance_key: &bc_jobs::domain::keys::InstanceKey,
) -> Result<Uuid, PortError> {
    sqlx::query_scalar(
        "SELECT ri.id::uuid FROM runner_instances ri \
         JOIN runner_types rt ON rt.id = ri.runner_type_id \
         WHERE rt.type_key = $1 AND ri.instance_key = $2",
    )
    .bind(runner_type.as_str())
    .bind(instance_key.as_str())
    .fetch_optional(&mut *tx)
    .await
    .map_err(unavailable)?
    .ok_or(PortError::ConcurrentModification)
}

async fn settle_reservation(
    tx: &mut PgConnection,
    run_id: Uuid,
    at: DateTime<Utc>,
) -> Result<(), PortError> {
    sqlx::query(
        "UPDATE run_reservations SET settled_at = $2 WHERE run_id = $1 AND settled_at IS NULL",
    )
    .bind(run_id)
    .bind(at)
    .execute(&mut *tx)
    .await
    .map_err(unavailable)?;
    Ok(())
}

pub async fn started(
    tx: &mut PgConnection,
    fact: &RunStarted,
    at: DateTime<Utc>,
) -> Result<(), PortError> {
    let runner_type = refs::runner_type_id(tx, &fact.runner_type).await?;
    let instance = refs::runner_instance_id(tx, runner_type, &fact.instance_key).await?;
    sqlx::query(
        "INSERT INTO run_starts (run_id, instance_id, started_at) VALUES ($1, $2, $3) \
         ON CONFLICT (run_id) DO NOTHING",
    )
    .bind(fact.run_id.as_uuid())
    .bind(instance)
    .bind(at)
    .execute(&mut *tx)
    .await
    .map_err(unavailable)?;
    super::waiting::trigger_settled(tx, fact.run_id.as_uuid()).await?;
    settle_reservation(tx, fact.run_id.as_uuid(), at).await
}

pub async fn plan_declared(
    tx: &mut PgConnection,
    fact: &RunPlanDeclared,
    at: DateTime<Utc>,
) -> Result<(), PortError> {
    sqlx::query(
        "INSERT INTO run_plan_declarations (id, run_id, declaration_number, declared_at) \
         VALUES ($1, $2, $3, $4)",
    )
    .bind(fact.declaration_id.as_uuid())
    .bind(fact.run_id.as_uuid())
    .bind(i32::try_from(fact.declaration_number.get()).unwrap_or(i32::MAX))
    .bind(at)
    .execute(&mut *tx)
    .await
    .map_err(unavailable)?;
    for item in &fact.items {
        sqlx::query(
            "INSERT INTO run_plan_items (declaration_id, step_index, label) VALUES ($1, $2, $3)",
        )
        .bind(fact.declaration_id.as_uuid())
        .bind(i32::try_from(item.index().get()).unwrap_or(i32::MAX))
        .bind(item.label().as_str())
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
    }
    Ok(())
}

pub async fn step_started(tx: &mut PgConnection, fact: &RunStepStarted) -> Result<(), PortError> {
    sqlx::query(
        "INSERT INTO steps (run_id, step_index, label, started_at) VALUES ($1, $2, $3, $4) \
         ON CONFLICT (run_id, step_index) DO NOTHING",
    )
    .bind(fact.run_id.as_uuid())
    .bind(i32::try_from(fact.step_index.get()).unwrap_or(i32::MAX))
    .bind(fact.label.as_str())
    .bind(fact.started_at)
    .execute(&mut *tx)
    .await
    .map_err(unavailable)?;
    Ok(())
}

pub async fn completed(
    tx: &mut PgConnection,
    fact: &RunCompleted,
    at: DateTime<Utc>,
) -> Result<(), PortError> {
    terminal(tx, fact.run_id.as_uuid(), "COMPLETED", at, None).await
}

pub async fn cancelled(
    tx: &mut PgConnection,
    fact: &RunCancelled,
    at: DateTime<Utc>,
) -> Result<(), PortError> {
    terminal(tx, fact.run_id.as_uuid(), "CANCELLED", at, None).await?;
    let Some(reason) = &fact.withdrawn_for else {
        return Ok(());
    };
    sqlx::query("UPDATE runs SET withdrawn_reason = $2 WHERE id = $1")
        .bind(fact.run_id.as_uuid())
        .bind(reason.as_str())
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
    let due_at: Option<DateTime<Utc>> = sqlx::query_scalar(
        "SELECT s.due_at FROM runs r JOIN run_retry_schedules s \
            ON s.id = r.automatic_retry_schedule_id WHERE r.id = $1",
    )
    .bind(fact.run_id.as_uuid())
    .fetch_optional(&mut *tx)
    .await
    .map_err(unavailable)?;
    super::waiting::begin_waiting(tx, fact.job_id.as_uuid(), due_at).await
}

pub async fn failed(
    tx: &mut PgConnection,
    fact: &RunFailed,
    at: DateTime<Utc>,
) -> Result<(), PortError> {
    terminal(tx, fact.run_id.as_uuid(), "FAILED", at, Some(&fact.report)).await
}

async fn terminal(
    tx: &mut PgConnection,
    run_id: Uuid,
    kind: &str,
    at: DateTime<Utc>,
    report: Option<&bc_jobs::domain::run::failure::RunFailureReport>,
) -> Result<(), PortError> {
    sqlx::query(
        "INSERT INTO run_terminals \
         (run_id, kind, occurred_at, failure_kind, reason_code, params, diagnostic, \
          retry_after_hint) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, \
            CASE WHEN $8::bigint IS NULL THEN NULL \
                 ELSE make_interval(secs => $8::bigint) END) \
         ON CONFLICT (run_id) DO NOTHING",
    )
    .bind(run_id)
    .bind(kind)
    .bind(at)
    .bind(report.map(|report| report.kind().as_db_str()))
    .bind(report.map(|report| report.reason_code().as_str()))
    .bind(report.map(|report| report.params().clone()))
    .bind(report.map(|report| report.diagnostic().clone()))
    .bind(
        report
            .and_then(|report| report.retry_after())
            .map(|hint| hint.num_seconds()),
    )
    .execute(&mut *tx)
    .await
    .map_err(unavailable)?;
    sqlx::query("DELETE FROM current_runs WHERE run_id = $1")
        .bind(run_id)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
    super::waiting::trigger_settled(tx, run_id).await?;
    settle_reservation(tx, run_id, at).await?;
    crate::db::impacts::record_terminal(tx, run_id, at).await
}

pub async fn cancellation_requested(
    tx: &mut PgConnection,
    fact: &RunCancellationRequested,
    at: DateTime<Utc>,
) -> Result<(), PortError> {
    let requested_by = match &fact.requested_by {
        Some(user) => Some(refs::observe_known_user(tx, user, at).await?),
        None => None,
    };
    sqlx::query(
        "INSERT INTO run_cancellation_requests \
         (run_id, requested_at, reason_code, requested_by_id, originating_job_id) \
         VALUES ($1, $2, $3, $4, $5) ON CONFLICT (run_id) DO NOTHING",
    )
    .bind(fact.run_id.as_uuid())
    .bind(at)
    .bind(fact.reason_code.as_str())
    .bind(requested_by)
    .bind(fact.originating_job_id.map(|id| id.as_uuid()))
    .execute(&mut *tx)
    .await
    .map_err(unavailable)?;
    Ok(())
}

pub async fn retry_scheduled(
    tx: &mut PgConnection,
    fact: &RetryScheduled,
) -> Result<(), PortError> {
    sqlx::query("INSERT INTO run_retry_schedules (id, failed_run_id, due_at) VALUES ($1, $2, $3)")
        .bind(fact.schedule_id.as_uuid())
        .bind(fact.failed_run_id.as_uuid())
        .bind(fact.due_at)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
    super::waiting::begin_waiting(tx, fact.job_id.as_uuid(), Some(fact.due_at)).await
}
