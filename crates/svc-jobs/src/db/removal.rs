use bc_jobs::domain::directory::removal::DirectoryRemoval;
use bc_jobs::domain::ids::{AgentId, RunId};
use bc_jobs::ports::PortError;
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use super::acts_for::acts_for_chain;
use super::apply::directory::job_subtree_head;
use super::hydrate::unavailable;

const RUN: &str = "run";
const EMPTIED_ORIGIN: &str = "emptied_origin";
const ADDRESS: &str = "address";

fn removed_by_sql() -> String {
    format!(
        "{head}, chain_seed(run_id) AS (SELECT run_id FROM tree), {chain} \
         SELECT '{RUN}' AS kind, run_id::uuid AS id FROM tree \
         UNION ALL SELECT '{EMPTIED_ORIGIN}', c.run_id::uuid FROM chain c \
            WHERE NOT EXISTS (SELECT 1 FROM directory_agents d WHERE d.run_id = c.run_id) \
         UNION ALL SELECT '{ADDRESS}', agent_id::uuid FROM directory_agents \
            WHERE run_id IN (SELECT run_id FROM tree)",
        head = job_subtree_head(),
        chain = acts_for_chain!()
    )
}

pub async fn removed_with_jobs(
    tx: &mut PgConnection,
    roots: &[Uuid],
) -> Result<DirectoryRemoval, PortError> {
    if roots.is_empty() {
        return Ok(DirectoryRemoval::default());
    }
    let rows = sqlx::query(&removed_by_sql())
        .bind(roots)
        .fetch_all(&mut *tx)
        .await
        .map_err(unavailable)?;
    let (mut runs, mut emptied_origins, mut addresses) = (Vec::new(), Vec::new(), Vec::new());
    for row in rows {
        let kind: String = row.get("kind");
        let id: Uuid = row.get("id");
        match kind.as_str() {
            RUN => runs.push(RunId::new(id)?),
            EMPTIED_ORIGIN => emptied_origins.push(RunId::new(id)?),
            ADDRESS => addresses.push(AgentId::new(id)),
            other => {
                return Err(PortError::Unavailable {
                    detail: format!("a removal row of unknown kind {other}"),
                });
            }
        }
    }
    Ok(DirectoryRemoval::new(runs, emptied_origins, addresses))
}
