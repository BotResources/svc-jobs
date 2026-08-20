use bc_jobs::domain::actions::fleet::RETIREMENT_QUIET_PERIOD;
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
        "INSERT INTO runs (id, job_id, attempt_number, dispatched_at, automatic_retry_schedule_id) \
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(fact.run_id.as_uuid())
    .bind(fact.job_id.as_uuid())
    .bind(i32::try_from(fact.attempt_number.get()).unwrap_or(i32::MAX))
    .bind(at)
    .bind(fact.automatic_retry_schedule_id.map(|id| id.as_uuid()))
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
    Ok(())
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
    terminal(tx, fact.run_id.as_uuid(), "CANCELLED", at, None).await
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
    // Retirement holds this same canonical row while it re-evaluates its quiet-period facts.
    // Taking it before adding a terminal makes that decision and this fact linearizable: a
    // retirement can never pass on a snapshot that races a just-recorded terminal run.
    sqlx::query(
        "SELECT rt.id FROM runner_types rt \
         JOIN jobs j ON j.runner_type_id = rt.id \
         JOIN runs r ON r.job_id = j.id \
         WHERE r.id = $1 FOR UPDATE OF rt",
    )
    .bind(run_id)
    .fetch_one(&mut *tx)
    .await
    .map_err(unavailable)?;
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
    sqlx::query(
        "INSERT INTO runner_type_affordance_impacts (runner_type_id, eligible_at) \
         SELECT j.runner_type_id, $2 FROM runs r JOIN jobs j ON j.id = r.job_id \
         WHERE r.id = $1 ON CONFLICT (runner_type_id, eligible_at) DO NOTHING",
    )
    .bind(run_id)
    .bind(at + RETIREMENT_QUIET_PERIOD)
    .execute(&mut *tx)
    .await
    .map_err(unavailable)?;
    Ok(())
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
    Ok(())
}
