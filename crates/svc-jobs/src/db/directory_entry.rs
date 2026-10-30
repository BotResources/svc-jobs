use bc_jobs::domain::directory::AgentState;
use bc_jobs::domain::directory::listing::{DirectoryEntry, RestartObstacles};
use bc_jobs::domain::ids::{AgentId, RunId};
use bc_jobs::domain::keys::RunnerTypeKey;
use bc_jobs::ports::PortError;
use sqlx::Row;

pub const PARENT_TERMINAL: &str = "(j.parent_job_id IS NOT NULL AND EXISTS \
    (SELECT 1 FROM job_resolutions jr WHERE jr.job_id = j.parent_job_id))";

const RESTART_JOB_LIVE: &str = "EXISTS (SELECT 1 FROM restart_origins ro \
        JOIN jobs rj ON rj.id = ro.job_id \
        WHERE ro.restarts_run_id = d.run_id \
          AND NOT EXISTS (SELECT 1 FROM job_resolutions jr WHERE jr.job_id = rj.id) \
          AND NOT EXISTS (SELECT 1 FROM runs rr \
               JOIN run_terminals rt2 ON rt2.run_id = rr.id AND rt2.kind = 'COMPLETED' \
               WHERE rr.job_id = rj.id))";

const HOLDS_SAVED_STATE: &str =
    "EXISTS (SELECT 1 FROM saved_state_agents s WHERE s.agent_id = d.agent_id)";

pub fn entry_columns() -> String {
    format!(
        "d.agent_id::uuid AS agent_id, d.run_id::uuid AS run_id, rt.type_key, d.state, \
         d.has_inbox, d.declares_restart, {HOLDS_SAVED_STATE} AS holds_saved_state, \
         coalesce(reg.lifecycle = 'RETIRED', false) AS retired, \
         {PARENT_TERMINAL} AS parent_terminal, {RESTART_JOB_LIVE} AS restart_job_live"
    )
}

pub const ENTRY_TABLES: &str = "FROM directory_agents d \
     JOIN runs r ON r.id = d.run_id \
     JOIN jobs j ON j.id = r.job_id \
     JOIN runner_types rt ON rt.id = j.runner_type_id \
     LEFT JOIN registered_runner_types reg ON reg.runner_type_id = rt.id";

pub fn entry_of(
    row: &sqlx::postgres::PgRow,
) -> Result<(DirectoryEntry, RestartObstacles), PortError> {
    let entry = DirectoryEntry {
        agent_id: AgentId::new(row.get("agent_id")),
        run_id: RunId::new(row.get("run_id"))?,
        runner_type: RunnerTypeKey::new(row.get::<String, _>("type_key"))?,
        state: AgentState::from_db_str(&row.get::<String, _>("state"))?,
        has_inbox: row.get("has_inbox"),
        declares_restart: row.get("declares_restart"),
        holds_saved_state: row.get("holds_saved_state"),
    };
    let obstacles = RestartObstacles {
        runner_type_retired: row.get("retired"),
        parent_job_terminal: row.get("parent_terminal"),
        restart_job_live: row.get("restart_job_live"),
    };
    Ok((entry, obstacles))
}
