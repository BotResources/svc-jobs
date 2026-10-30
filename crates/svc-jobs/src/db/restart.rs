use bc_jobs::ports::PortError;
use chrono::{DateTime, Utc};
use sqlx::PgConnection;
use uuid::Uuid;

use super::PgStore;
use super::hydrate::unavailable;

const FAILURE_NOTICE: &str = "i.kind = 'NOTICE' AND i.notice ->> 'type' = 'JOB_FAILED'";

fn due_entry() -> String {
    format!(
        "d.state = 'FINISHED' AND d.declares_restart \
         AND EXISTS (SELECT 1 FROM saved_state_agents s WHERE s.agent_id = d.agent_id) \
         AND (EXISTS (SELECT 1 FROM inbox_items i WHERE i.address = d.agent_id \
                 AND i.read_at IS NULL AND (i.kind = 'MAIL' OR ({FAILURE_NOTICE}))) \
              OR EXISTS (SELECT 1 FROM inbox_items i WHERE i.address IS NULL \
                 AND i.run_id = d.run_id AND i.read_at IS NULL AND {FAILURE_NOTICE})) \
         AND NOT EXISTS (SELECT 1 FROM restart_origins ro \
                 JOIN jobs rj ON rj.id = ro.job_id \
                 WHERE ro.restarts_run_id = d.run_id \
                   AND NOT EXISTS (SELECT 1 FROM job_resolutions jr WHERE jr.job_id = rj.id) \
                   AND NOT EXISTS (SELECT 1 FROM runs rr \
                        JOIN run_terminals rt ON rt.run_id = rr.id AND rt.kind = 'COMPLETED' \
                        WHERE rr.job_id = rj.id))"
    )
}

pub async fn wake_addresses(
    tx: &mut PgConnection,
    addresses: &[Uuid],
    at: DateTime<Utc>,
) -> Result<(), PortError> {
    let sql = format!(
        "INSERT INTO restart_wakes (run_id, requested_at) \
         SELECT DISTINCT d.run_id, $2 FROM directory_agents d \
         WHERE d.agent_id = ANY($1) AND {} ON CONFLICT (run_id) DO NOTHING",
        due_entry()
    );
    sqlx::query(&sql)
        .bind(addresses)
        .bind(at)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
    Ok(())
}

pub async fn wake_run(
    tx: &mut PgConnection,
    run_id: Uuid,
    at: DateTime<Utc>,
) -> Result<(), PortError> {
    let sql = format!(
        "INSERT INTO restart_wakes (run_id, requested_at) \
         SELECT DISTINCT d.run_id, $2 FROM directory_agents d \
         WHERE d.run_id = $1 AND {} ON CONFLICT (run_id) DO NOTHING",
        due_entry()
    );
    sqlx::query(&sql)
        .bind(run_id)
        .bind(at)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
    Ok(())
}

pub async fn restart_still_due(tx: &mut PgConnection, origin_run: Uuid) -> Result<bool, PortError> {
    let sql = format!(
        "SELECT EXISTS (SELECT 1 FROM directory_agents d WHERE d.run_id = $1 AND {})",
        due_entry()
    );
    sqlx::query_scalar(&sql)
        .bind(origin_run)
        .fetch_one(&mut *tx)
        .await
        .map_err(unavailable)
}

pub async fn consume_wake(tx: &mut PgConnection, origin_run: Uuid) -> Result<(), PortError> {
    sqlx::query("DELETE FROM restart_wakes WHERE run_id = $1")
        .bind(origin_run)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
    Ok(())
}

impl PgStore {
    pub async fn due_wakes(&self) -> Result<Vec<Uuid>, PortError> {
        sqlx::query_scalar("SELECT run_id::uuid FROM restart_wakes ORDER BY requested_at, run_id")
            .fetch_all(self.pool())
            .await
            .map_err(unavailable)
    }

    pub async fn drop_wake(&self, origin_run: Uuid) -> Result<(), PortError> {
        let mut connection = self.pool().acquire().await.map_err(unavailable)?;
        consume_wake(&mut connection, origin_run).await
    }
}
