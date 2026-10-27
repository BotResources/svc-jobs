//! The `waiting_jobs` access path and the per-root dispatch turns, written in
//! the transaction of the fact that changes them — never a separately
//! synchronized read model: every row is derivable from the facts beside it.

use bc_jobs::ports::PortError;
use chrono::{DateTime, Utc};
use sqlx::PgConnection;
use uuid::Uuid;

use crate::db::hydrate::unavailable;
use crate::db::lineage::root_of;

/// The Job starts waiting: queued (`due_at` absent) or for a retry due at
/// `due_at`.
pub async fn begin_waiting(
    tx: &mut PgConnection,
    job_id: Uuid,
    due_at: Option<DateTime<Utc>>,
) -> Result<(), PortError> {
    let sql = format!(
        "INSERT INTO waiting_jobs (job_id, runner_type_id, root_job_id, effective_priority, \
            created_at, due_at) \
         SELECT j.id, j.runner_type_id, {}, j.effective_priority, j.created_at, $2 \
         FROM jobs j WHERE j.id = $1 \
         ON CONFLICT (job_id) DO UPDATE SET due_at = EXCLUDED.due_at, \
            effective_priority = EXCLUDED.effective_priority",
        root_of("j"),
    );
    sqlx::query(&sql)
        .bind(job_id)
        .bind(due_at)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
    Ok(())
}

/// The wait ends: a Run was dispatched or reserved, or the Job resolved.
pub async fn end_waiting(tx: &mut PgConnection, job_id: Uuid) -> Result<(), PortError> {
    sqlx::query("DELETE FROM waiting_jobs WHERE job_id = $1")
        .bind(job_id)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
    Ok(())
}

pub async fn reprioritize(
    tx: &mut PgConnection,
    job_id: Uuid,
    effective_priority: i16,
) -> Result<(), PortError> {
    sqlx::query("UPDATE waiting_jobs SET effective_priority = $2 WHERE job_id = $1")
        .bind(job_id)
        .bind(effective_priority)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
    Ok(())
}

/// The Job's root was just served, for the Job's RunnerType: the next Job of
/// this root in this type waits for every other root's turn.
pub async fn served(
    tx: &mut PgConnection,
    job_id: Uuid,
    at: DateTime<Utc>,
) -> Result<(), PortError> {
    let sql = format!(
        "INSERT INTO root_dispatch_turns (runner_type_id, root_job_id, last_served_at) \
         SELECT j.runner_type_id, {}, $2 \
         FROM jobs j WHERE j.id = $1 \
         ON CONFLICT (runner_type_id, root_job_id) \
         DO UPDATE SET last_served_at = greatest(root_dispatch_turns.last_served_at, \
            EXCLUDED.last_served_at)",
        root_of("j"),
    );
    sqlx::query(&sql)
        .bind(job_id)
        .bind(at)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
    Ok(())
}

/// A trigger was published for the Run: it is unstarted until an Instance
/// takes it or it settles.
pub async fn trigger_published(
    tx: &mut PgConnection,
    run_id: Uuid,
    job_id: Uuid,
    at: DateTime<Utc>,
) -> Result<(), PortError> {
    sqlx::query(
        "INSERT INTO unstarted_pushed_runs (run_id, job_id, runner_type_id, dispatched_at) \
         SELECT $1, j.id, j.runner_type_id, $3 FROM jobs j WHERE j.id = $2 \
         ON CONFLICT (run_id) DO NOTHING",
    )
    .bind(run_id)
    .bind(job_id)
    .bind(at)
    .execute(&mut *tx)
    .await
    .map_err(unavailable)?;
    Ok(())
}

/// The Run was taken or settled: its trigger is no longer outstanding.
pub async fn trigger_settled(tx: &mut PgConnection, run_id: Uuid) -> Result<(), PortError> {
    sqlx::query("DELETE FROM unstarted_pushed_runs WHERE run_id = $1")
        .bind(run_id)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
    Ok(())
}
