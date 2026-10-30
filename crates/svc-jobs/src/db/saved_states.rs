use async_trait::async_trait;
use bc_jobs::domain::ids::{AgentId, RunId};
use bc_jobs::domain::runner::refusal::RunnerRefusal;
use bc_jobs::domain::runner::saved_state::{SavedState, SavedStateRefusal};
use bc_jobs::domain::runner::views::SavedStateView;
use bc_jobs::domain::runner::work::WorkNamed;
use bc_jobs::ports::PortError;
use bc_jobs::ports::saved_state::SavedStates;
use chrono::{DateTime, Utc};
use sqlx::Row;
use uuid::Uuid;

use super::PgStore;
use super::hydrate::unavailable;
use super::work::require_work;

const SAVED_STATE_RUN_SQL: &str = "WITH RECURSIVE chain(run_id, depth) AS ( \
        SELECT ro.restarts_run_id, 1 FROM runs r \
        JOIN restart_origins ro ON ro.job_id = r.job_id WHERE r.id = $1 \
      UNION ALL \
        SELECT ro.restarts_run_id, c.depth + 1 FROM chain c \
        JOIN runs r ON r.id = c.run_id \
        JOIN restart_origins ro ON ro.job_id = r.job_id) \
    SELECT c.run_id::uuid FROM chain c \
    JOIN saved_states s ON s.run_id = c.run_id ORDER BY c.depth LIMIT 1";

pub async fn saved_state_run_in(
    tx: &mut sqlx::PgConnection,
    restart_run: RunId,
) -> Result<Option<RunId>, PortError> {
    let found: Option<Uuid> = sqlx::query_scalar(SAVED_STATE_RUN_SQL)
        .bind(restart_run.as_uuid())
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?;
    found.map(RunId::new).transpose().map_err(PortError::from)
}

#[async_trait]
impl SavedStates for PgStore {
    async fn save_state(
        &self,
        work: WorkNamed<'_>,
        state: &SavedState,
        at: DateTime<Utc>,
    ) -> Result<Result<(), RunnerRefusal>, PortError> {
        let mut tx = self.begin().await?;
        let context = match require_work(&mut tx, work, true).await? {
            Ok(context) => context,
            Err(refusal) => return Ok(Err(refusal.into())),
        };
        sqlx::query(
            "INSERT INTO saved_states (run_id, state, saved_at) VALUES ($1, $2, $3) \
             ON CONFLICT (run_id) DO UPDATE SET state = EXCLUDED.state, saved_at = EXCLUDED.saved_at",
        )
        .bind(context.run_id.as_uuid())
        .bind(state.as_value())
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

    async fn read_saved_state(
        &self,
        work: WorkNamed<'_>,
    ) -> Result<Result<SavedStateView, RunnerRefusal>, PortError> {
        let mut tx = self.begin().await?;
        let context = match require_work(&mut tx, work, false).await? {
            Ok(context) => context,
            Err(refusal) => return Ok(Err(refusal.into())),
        };
        let not_a_restart = || RunnerRefusal::from(SavedStateRefusal::NotARestartRun);
        let Some(restarted) = saved_state_run_in(&mut tx, context.run_id).await? else {
            return Ok(Err(not_a_restart()));
        };
        let state = sqlx::query("SELECT state FROM saved_states WHERE run_id = $1")
            .bind(restarted.as_uuid())
            .fetch_one(&mut *tx)
            .await
            .map_err(unavailable)?
            .get::<serde_json::Value, _>("state");
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
