use bc_jobs::domain::directory::removal::DirectoryRemoval;
use bc_jobs::domain::ids::{AgentId, RunId};
use bc_jobs::ports::PortError;
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use super::apply::directory::job_subtree_head;
use super::hydrate::unavailable;

const REMOVED_BY_SQL_TAIL: &str = ", \
     acting_for(run_id) AS ( \
        SELECT ro.restarts_run_id FROM tree t \
        JOIN runs r ON r.id = t.run_id \
        JOIN restart_origins ro ON ro.job_id = r.job_id \
        WHERE EXISTS (SELECT 1 FROM directory_agents d WHERE d.run_id = t.run_id) \
      UNION \
        SELECT ro.restarts_run_id FROM acting_for a \
        JOIN runs r ON r.id = a.run_id \
        JOIN restart_origins ro ON ro.job_id = r.job_id) \
     SELECT 'run' AS kind, run_id::uuid AS id FROM tree \
     UNION ALL SELECT 'acted_for', run_id::uuid FROM acting_for \
     UNION ALL SELECT 'address', agent_id::uuid FROM directory_agents \
        WHERE run_id IN (SELECT run_id FROM tree)";

pub async fn removed_with_jobs(
    tx: &mut PgConnection,
    roots: &[Uuid],
) -> Result<DirectoryRemoval, PortError> {
    if roots.is_empty() {
        return Ok(DirectoryRemoval::default());
    }
    let sql = format!("{}{REMOVED_BY_SQL_TAIL}", job_subtree_head());
    let rows = sqlx::query(&sql)
        .bind(roots)
        .fetch_all(&mut *tx)
        .await
        .map_err(unavailable)?;
    let (mut runs, mut acted_for, mut addresses) = (Vec::new(), Vec::new(), Vec::new());
    for row in rows {
        let kind: String = row.get("kind");
        let id: Uuid = row.get("id");
        match kind.as_str() {
            "run" => runs.push(RunId::new(id)?),
            "acted_for" => acted_for.push(RunId::new(id)?),
            _ => addresses.push(AgentId::new(id)),
        }
    }
    Ok(DirectoryRemoval::new(runs, acted_for, addresses))
}
