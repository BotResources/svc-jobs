use bc_jobs::domain::ids::{AgentId, RunId};
use bc_jobs::domain::runner::refusal::RunnerRefusal;
use bc_jobs::domain::runner::saved_state::{SavedStateRefusal, guard_state_within};
use bc_jobs::ports::PortError;
use chrono::{DateTime, Utc};
use sqlx::Row;
use uuid::Uuid;

use super::PgStore;
use super::hydrate::unavailable;
use super::work::{WorkNamed, require_work};

#[derive(Debug, Clone, PartialEq)]
pub struct SavedStateView {
    pub state: serde_json::Value,
    pub agent_ids: Vec<AgentId>,
    pub mail_addresses: Vec<AgentId>,
}

impl PgStore {
    pub async fn save_state(
        &self,
        work: WorkNamed<'_>,
        state: &serde_json::Value,
        maximum_bytes: usize,
        at: DateTime<Utc>,
    ) -> Result<Result<(), RunnerRefusal>, PortError> {
        let mut tx = self.begin().await?;
        let context = match require_work(&mut tx, work, false).await? {
            Ok(context) => context,
            Err(refusal) => return Ok(Err(refusal.into())),
        };
        let size = serde_json::to_vec(state).map_or(usize::MAX, |bytes| bytes.len());
        if let Err(refusal) = guard_state_within(size, maximum_bytes) {
            return Ok(Err(refusal.into()));
        }
        sqlx::query(
            "INSERT INTO saved_states (run_id, state, saved_at) VALUES ($1, $2, $3) \
             ON CONFLICT (run_id) DO UPDATE SET state = EXCLUDED.state, saved_at = EXCLUDED.saved_at",
        )
        .bind(context.run_id.as_uuid())
        .bind(state)
        .bind(at)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
        sqlx::query("DELETE FROM saved_state_agents WHERE run_id = $1")
            .bind(context.run_id.as_uuid())
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
        sqlx::query(
            "INSERT INTO saved_state_agents (run_id, agent_id, has_inbox) \
             SELECT run_id, agent_id, has_inbox FROM directory_agents WHERE run_id = $1",
        )
        .bind(context.run_id.as_uuid())
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
        tx.commit().await.map_err(unavailable)?;
        Ok(Ok(()))
    }

    pub async fn read_saved_state(
        &self,
        work: WorkNamed<'_>,
    ) -> Result<Result<SavedStateView, RunnerRefusal>, PortError> {
        let mut tx = self.begin().await?;
        let context = match require_work(&mut tx, work, false).await? {
            Ok(context) => context,
            Err(refusal) => return Ok(Err(refusal.into())),
        };
        let restarted: Option<Uuid> = sqlx::query_scalar(
            "SELECT restarts_run_id::uuid FROM restart_origins WHERE job_id = $1",
        )
        .bind(context.job_id.as_uuid())
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?;
        let not_a_restart = || RunnerRefusal::from(SavedStateRefusal::NotARestartRun);
        let Some(restarted) = restarted else {
            return Ok(Err(not_a_restart()));
        };
        let restarted = RunId::new(restarted)?;
        let Some(state) = sqlx::query("SELECT state FROM saved_states WHERE run_id = $1")
            .bind(restarted.as_uuid())
            .fetch_optional(&mut *tx)
            .await
            .map_err(unavailable)?
            .map(|row| row.get::<serde_json::Value, _>("state"))
        else {
            return Ok(Err(not_a_restart()));
        };
        let agents = sqlx::query(
            "SELECT agent_id, has_inbox FROM saved_state_agents WHERE run_id = $1 ORDER BY agent_id",
        )
        .bind(restarted.as_uuid())
        .fetch_all(&mut *tx)
        .await
        .map_err(unavailable)?;
        Ok(Ok(SavedStateView {
            state,
            agent_ids: agents
                .iter()
                .map(|row| AgentId::new(row.get("agent_id")))
                .collect(),
            mail_addresses: agents
                .iter()
                .filter(|row| row.get::<bool, _>("has_inbox"))
                .map(|row| AgentId::new(row.get("agent_id")))
                .collect(),
        }))
    }
}
