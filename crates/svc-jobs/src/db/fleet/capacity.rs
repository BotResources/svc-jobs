use bc_jobs::domain::keys::RunnerTypeKey;
use bc_jobs::ports::PortError;
use sqlx::PgConnection;
use uuid::Uuid;

use super::load;
use crate::db::hydrate::unavailable;

pub struct TypeCapacity {
    pub runner_type: bc_jobs::domain::fleet::RunnerType,
    pub current_runs: u32,
}

pub async fn lock_and_read(
    tx: &mut PgConnection,
    key: &RunnerTypeKey,
) -> Result<Option<TypeCapacity>, PortError> {
    let Some(route) = load::route_id(tx, key).await? else {
        return Ok(None);
    };
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(capacity_lock_key(route))
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
    let Some(runner_type) = load::load_one(tx, key).await? else {
        return Ok(None);
    };
    let current_runs: i64 =
        sqlx::query_scalar("SELECT count(*) FROM current_runs WHERE runner_type_id = $1")
            .bind(route)
            .fetch_one(&mut *tx)
            .await
            .map_err(unavailable)?;
    Ok(Some(TypeCapacity {
        runner_type,
        current_runs: u32::try_from(current_runs).unwrap_or(u32::MAX),
    }))
}

fn capacity_lock_key(route: Uuid) -> String {
    format!("capacity:{route}")
}
