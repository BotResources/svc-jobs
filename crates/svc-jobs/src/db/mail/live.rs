use bc_jobs::domain::ids::{AgentId, RunId};
use bc_jobs::ports::PortError;
use sqlx::PgConnection;
use uuid::Uuid;

use crate::db::hydrate::unavailable;

pub(super) async fn live_run_of(
    tx: &mut PgConnection,
    address: AgentId,
) -> Result<Option<RunId>, PortError> {
    let found: Option<Uuid> = sqlx::query_scalar(
        "SELECT d.run_id::uuid FROM directory_agents d \
         WHERE d.agent_id = $1 AND d.state = 'RUNNING' \
         UNION ALL \
         SELECT r.id::uuid FROM directory_agents d \
         JOIN restart_origins ro ON ro.restarts_run_id = d.run_id \
         JOIN runs r ON r.job_id = ro.job_id \
         WHERE d.agent_id = $1 AND d.state = 'FINISHED' \
           AND EXISTS (SELECT 1 FROM run_starts s WHERE s.run_id = r.id) \
           AND NOT EXISTS (SELECT 1 FROM run_terminals t WHERE t.run_id = r.id) \
         LIMIT 1",
    )
    .bind(address.as_uuid())
    .fetch_optional(&mut *tx)
    .await
    .map_err(unavailable)?;
    found.map(RunId::new).transpose().map_err(PortError::from)
}
