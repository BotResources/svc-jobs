use async_trait::async_trait;
use bc_jobs::JobsError;
use bc_jobs::domain::directory::AgentState;
use bc_jobs::domain::directory::listing::DirectoryEntry;
use bc_jobs::domain::directory::registration::{
    AgentDeclaration, HeldAgent, RegistrationRefusal, RunStanding,
};
use bc_jobs::domain::ids::{AgentId, JobId, RunId};
use bc_jobs::domain::keys::RunnerTypeKey;
use bc_jobs::ports::PortError;
use bc_jobs::ports::directory::{
    AgentDirectory, ListedEntry, RegistrationDecision, RunCaller, StandingDecision,
};
use chrono::{DateTime, Utc};
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use super::PgStore;
use super::hydrate::unavailable;

pub const PARENT_TERMINAL: &str = "(j.parent_job_id IS NOT NULL AND EXISTS \
    (SELECT 1 FROM job_resolutions jr WHERE jr.job_id = j.parent_job_id))";

const STANDING_SQL: &str = "SELECT rt.type_key, ri.instance_key, \
        (rs.run_id IS NOT NULL) AS started, (t.run_id IS NOT NULL) AS ended, \
        (dr.run_id IS NOT NULL) AS removed, ({root})::uuid AS initial_job_id \
     FROM runs r \
     JOIN jobs j ON j.id = r.job_id \
     JOIN runner_types rt ON rt.id = j.runner_type_id \
     LEFT JOIN run_starts rs ON rs.run_id = r.id \
     LEFT JOIN runner_instances ri ON ri.id = rs.instance_id \
     LEFT JOIN run_terminals t ON t.run_id = r.id \
     LEFT JOIN directory_removed_runs dr ON dr.run_id = r.id \
     WHERE r.id = $1";

pub async fn standing(
    tx: &mut PgConnection,
    run_id: RunId,
    runner_type: Option<&str>,
    instance_key: &str,
) -> Result<RunStanding, PortError> {
    let sql = STANDING_SQL.replace("{root}", &super::lineage::root_of("j"));
    let Some(row) = sqlx::query(&sql)
        .bind(run_id.as_uuid())
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?
    else {
        return Ok(RunStanding::Unknown);
    };
    if row.get::<bool, _>("removed") {
        return Ok(RunStanding::Removed);
    }
    if !row.get::<bool, _>("started") {
        return Ok(RunStanding::NotStarted);
    }
    if row.get::<bool, _>("ended") {
        return Ok(RunStanding::Ended);
    }
    let held_by: Option<String> = row.get("instance_key");
    let type_matches =
        runner_type.is_none_or(|expected| row.get::<String, _>("type_key") == expected);
    if !type_matches || held_by.as_deref() != Some(instance_key) {
        return Ok(RunStanding::StartedOnAnotherInstance);
    }
    Ok(RunStanding::Started {
        initial_job_id: JobId::new(row.get("initial_job_id"))?,
    })
}

pub async fn lock_tree(tx: &mut PgConnection, initial_job_id: JobId) -> Result<(), PortError> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(format!("directory:{}", initial_job_id.as_uuid()))
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
    Ok(())
}

#[async_trait]
impl AgentDirectory for PgStore {
    async fn register(
        &self,
        caller: RunCaller<'_>,
        declarations: &[AgentDeclaration],
        decide: RegistrationDecision<'_>,
        at: DateTime<Utc>,
    ) -> Result<Result<(), RegistrationRefusal>, PortError> {
        let mut tx = self.begin().await?;
        let first = caller_standing(&mut tx, &caller).await?;
        if let RunStanding::Started { initial_job_id } = first {
            lock_tree(&mut tx, initial_job_id).await?;
        }
        let current = caller_standing(&mut tx, &caller).await?;
        let held = held_agents(&mut tx, declarations).await?;
        let registration = match decide(current, &held) {
            Ok(registration) => registration,
            Err(refusal) => return Ok(Err(refusal)),
        };
        for placed in &registration.agents {
            upsert(&mut tx, &registration, &placed.declaration, at).await?;
        }
        tx.commit().await.map_err(unavailable)?;
        Ok(Ok(()))
    }

    async fn list(
        &self,
        caller: RunCaller<'_>,
        decide: StandingDecision<'_>,
    ) -> Result<Result<Vec<ListedEntry>, RegistrationRefusal>, PortError> {
        let mut tx = self.begin().await?;
        let current = caller_standing(&mut tx, &caller).await?;
        let initial_job_id = match decide(current) {
            Ok(initial_job_id) => initial_job_id,
            Err(refusal) => return Ok(Err(refusal)),
        };
        let rows = sqlx::query(
            &"SELECT d.agent_id::uuid AS agent_id, d.run_id::uuid AS run_id, rt.type_key, \
                d.state, d.has_inbox, d.declares_restart, \
                EXISTS (SELECT 1 FROM saved_state_agents s \
                        WHERE s.agent_id = d.agent_id) AS holds_saved_state, \
                coalesce(reg.lifecycle = 'RETIRED', false) AS retired, \
                {PARENT_TERMINAL} AS parent_terminal \
             FROM directory_agents d \
             JOIN runs r ON r.id = d.run_id \
             JOIN jobs j ON j.id = r.job_id \
             JOIN runner_types rt ON rt.id = j.runner_type_id \
             LEFT JOIN registered_runner_types reg ON reg.runner_type_id = rt.id \
             WHERE d.initial_job_id = $1 \
             ORDER BY d.registered_at, d.agent_id"
                .replace("{PARENT_TERMINAL}", PARENT_TERMINAL),
        )
        .bind(initial_job_id.as_uuid())
        .fetch_all(&mut *tx)
        .await
        .map_err(unavailable)?;
        rows.iter().map(listed).collect::<Result<_, _>>().map(Ok)
    }
}

async fn caller_standing(
    tx: &mut PgConnection,
    caller: &RunCaller<'_>,
) -> Result<RunStanding, PortError> {
    standing(
        tx,
        caller.run_id,
        Some(caller.runner_type),
        caller.instance_key,
    )
    .await
}

impl PgStore {
    pub async fn awaiting_start(&self, run_id: RunId) -> Result<Option<RunnerTypeKey>, PortError> {
        let found: Option<String> = sqlx::query_scalar(
            "SELECT rt.type_key FROM runs r \
             JOIN jobs j ON j.id = r.job_id \
             JOIN runner_types rt ON rt.id = j.runner_type_id \
             WHERE r.id = $1 \
               AND NOT EXISTS (SELECT 1 FROM run_starts s WHERE s.run_id = r.id) \
               AND NOT EXISTS (SELECT 1 FROM run_terminals t WHERE t.run_id = r.id) \
               AND NOT EXISTS (SELECT 1 FROM directory_removed_runs d WHERE d.run_id = r.id)",
        )
        .bind(run_id.as_uuid())
        .fetch_optional(self.pool())
        .await
        .map_err(unavailable)?;
        found
            .map(RunnerTypeKey::new)
            .transpose()
            .map_err(PortError::from)
    }

    pub async fn mail_address_run(&self, address: Uuid) -> Result<Option<RunId>, PortError> {
        let found: Option<Uuid> = sqlx::query_scalar(
            "SELECT run_id::uuid FROM directory_agents WHERE agent_id = $1 AND has_inbox",
        )
        .bind(address)
        .fetch_optional(self.pool())
        .await
        .map_err(unavailable)?;
        found.map(RunId::new).transpose().map_err(PortError::from)
    }
}

fn listed(row: &sqlx::postgres::PgRow) -> Result<ListedEntry, PortError> {
    Ok(ListedEntry {
        entry: DirectoryEntry {
            agent_id: AgentId::new(row.get("agent_id")),
            run_id: RunId::new(row.get("run_id"))?,
            runner_type: RunnerTypeKey::new(row.get::<String, _>("type_key"))?,
            state: AgentState::from_db_str(&row.get::<String, _>("state"))?,
            has_inbox: row.get("has_inbox"),
            declares_restart: row.get("declares_restart"),
            holds_saved_state: row.get("holds_saved_state"),
        },
        runner_type_retired: row.get("retired"),
        parent_job_terminal: row.get("parent_terminal"),
    })
}

async fn held_agents(
    tx: &mut PgConnection,
    declarations: &[AgentDeclaration],
) -> Result<Vec<HeldAgent>, PortError> {
    let ids: Vec<Uuid> = declarations
        .iter()
        .map(|declaration| declaration.agent_id.as_uuid())
        .collect();
    let rows = sqlx::query(
        "SELECT agent_id::uuid AS agent_id, run_id::uuid AS run_id, \
            initial_job_id::uuid AS initial_job_id, state \
         FROM directory_agents WHERE agent_id = ANY($1)",
    )
    .bind(&ids)
    .fetch_all(&mut *tx)
    .await
    .map_err(unavailable)?;
    rows.iter()
        .map(|row| {
            Ok(HeldAgent {
                agent_id: AgentId::new(row.get("agent_id")),
                run_id: RunId::new(row.get("run_id"))?,
                initial_job_id: JobId::new(row.get("initial_job_id"))?,
                state: AgentState::from_db_str(&row.get::<String, _>("state"))?,
            })
        })
        .collect::<Result<_, JobsError>>()
        .map_err(PortError::from)
}

async fn upsert(
    tx: &mut PgConnection,
    registration: &bc_jobs::domain::directory::registration::Registration,
    declaration: &AgentDeclaration,
    at: DateTime<Utc>,
) -> Result<(), PortError> {
    let written = sqlx::query(
        "INSERT INTO directory_agents \
            (agent_id, run_id, initial_job_id, has_inbox, declares_restart, state, registered_at) \
         VALUES ($1, $2, $3, $4, $5, 'RUNNING', $6) \
         ON CONFLICT (agent_id) DO UPDATE SET run_id = EXCLUDED.run_id, \
            has_inbox = EXCLUDED.has_inbox, declares_restart = EXCLUDED.declares_restart, \
            state = 'RUNNING' \
         WHERE directory_agents.initial_job_id = EXCLUDED.initial_job_id",
    )
    .bind(declaration.agent_id.as_uuid())
    .bind(registration.run_id.as_uuid())
    .bind(registration.initial_job_id.as_uuid())
    .bind(declaration.has_inbox)
    .bind(declaration.declares_restart)
    .bind(at)
    .execute(&mut *tx)
    .await
    .map_err(unavailable)?;
    if written.rows_affected() == 0 {
        return Err(PortError::ConcurrentModification);
    }
    sqlx::query("DELETE FROM directory_removed_addresses WHERE agent_id = $1")
        .bind(declaration.agent_id.as_uuid())
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
    Ok(())
}
