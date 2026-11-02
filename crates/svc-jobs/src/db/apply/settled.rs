use bc_jobs::ports::PortError;
use chrono::{DateTime, Utc};
use sqlx::PgConnection;
use uuid::Uuid;

use crate::db::hydrate::unavailable;

pub async fn settle(
    tx: &mut PgConnection,
    job_id: Uuid,
    at: DateTime<Utc>,
) -> Result<(), PortError> {
    sqlx::query("UPDATE jobs SET settled_at = coalesce(settled_at, $2) WHERE id = $1")
        .bind(job_id)
        .bind(at)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
    Ok(())
}
