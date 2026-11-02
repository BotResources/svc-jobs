use bc_jobs::domain::ids::{CommandId, JobId};
use bc_jobs::ports::PortError;
use sqlx::PgConnection;

use crate::db::hydrate::unavailable;

pub async fn remember(
    tx: &mut PgConnection,
    job_id: JobId,
    command_id: CommandId,
) -> Result<(), PortError> {
    let updated = sqlx::query(
        "UPDATE job_resolutions SET command_id = $2 WHERE job_id = $1 AND command_id IS NULL",
    )
    .bind(job_id.as_uuid())
    .bind(command_id.as_uuid())
    .execute(&mut *tx)
    .await
    .map_err(unavailable)?;
    if updated.rows_affected() == 1 {
        Ok(())
    } else {
        Err(PortError::ConcurrentModification)
    }
}
