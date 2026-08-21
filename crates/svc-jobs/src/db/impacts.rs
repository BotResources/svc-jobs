use bc_jobs::domain::ids::RunnerTypeId;
use bc_jobs::domain::keys::RunnerTypeKey;
use bc_jobs::ports::PortError;
use chrono::{DateTime, TimeDelta, Utc};
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use crate::db::hydrate::unavailable;

pub struct DueImpact {
    pub runner_type_id: RunnerTypeId,
    pub runner_type: RunnerTypeKey,
}

pub async fn record_terminal(
    tx: &mut PgConnection,
    run_id: Uuid,
    terminal_run_at: DateTime<Utc>,
) -> Result<(), PortError> {
    sqlx::query(
        "INSERT INTO runner_type_affordance_impacts (runner_type_id, terminal_run_at) \
         SELECT j.runner_type_id, $2 FROM runs r JOIN jobs j ON j.id = r.job_id \
         WHERE r.id = $1 ON CONFLICT (runner_type_id, terminal_run_at) DO NOTHING",
    )
    .bind(run_id)
    .bind(terminal_run_at)
    .execute(&mut *tx)
    .await
    .map_err(unavailable)?;
    Ok(())
}

pub async fn next_due(
    tx: &mut PgConnection,
    at: DateTime<Utc>,
    quiet_period: TimeDelta,
) -> Result<Option<DueImpact>, PortError> {
    let row = sqlx::query(
        "SELECT impact.runner_type_id::uuid AS runner_type_id, rt.type_key \
         FROM runner_type_affordance_impacts impact \
         JOIN registered_runner_types registered \
              ON registered.runner_type_id = impact.runner_type_id \
         JOIN runner_types rt ON rt.id = impact.runner_type_id \
         WHERE impact.terminal_run_at <= $1 \
         ORDER BY impact.terminal_run_at, impact.runner_type_id \
         LIMIT 1",
    )
    .bind(at - quiet_period)
    .fetch_optional(&mut *tx)
    .await
    .map_err(unavailable)?;
    let Some(row) = row else {
        return Ok(None);
    };
    Ok(Some(DueImpact {
        runner_type_id: RunnerTypeId::new(row.get("runner_type_id"))?,
        runner_type: RunnerTypeKey::new(row.get::<String, _>("type_key"))?,
    }))
}

pub async fn consume_due(
    tx: &mut PgConnection,
    due: &DueImpact,
    at: DateTime<Utc>,
    quiet_period: TimeDelta,
) -> Result<u64, PortError> {
    sqlx::query("SELECT id FROM runner_types WHERE id = $1 FOR UPDATE")
        .bind(due.runner_type_id.as_uuid())
        .fetch_one(&mut *tx)
        .await
        .map_err(unavailable)?;
    let consumed = sqlx::query(
        "DELETE FROM runner_type_affordance_impacts \
         WHERE runner_type_id = $1 AND terminal_run_at <= $2",
    )
    .bind(due.runner_type_id.as_uuid())
    .bind(at - quiet_period)
    .execute(&mut *tx)
    .await
    .map_err(unavailable)?;
    Ok(consumed.rows_affected())
}
