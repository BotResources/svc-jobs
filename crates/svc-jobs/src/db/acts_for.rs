use bc_jobs::domain::ids::RunId;
use bc_jobs::domain::requester::ActsFor;
use bc_jobs::ports::PortError;
use sqlx::PgConnection;
use uuid::Uuid;

use super::PgStore;
use super::hydrate::unavailable;

pub const ACTS_FOR_SQL: &str = "WITH RECURSIVE chain(run_id) AS ( \
        SELECT ro.restarts_run_id FROM runs r \
        JOIN restart_origins ro ON ro.job_id = r.job_id WHERE r.id = $1 \
      UNION \
        SELECT ro.restarts_run_id FROM chain c \
        JOIN runs r ON r.id = c.run_id \
        JOIN restart_origins ro ON ro.job_id = r.job_id) \
    SELECT run_id::uuid FROM chain";

pub async fn acts_for_in(tx: &mut PgConnection, run_id: RunId) -> Result<ActsFor, PortError> {
    let runs: Vec<Uuid> = sqlx::query_scalar(ACTS_FOR_SQL)
        .bind(run_id.as_uuid())
        .fetch_all(&mut *tx)
        .await
        .map_err(unavailable)?;
    runs.into_iter()
        .map(|run| RunId::new(run).map_err(PortError::from))
        .collect::<Result<Vec<_>, _>>()
        .map(ActsFor::new)
}

impl PgStore {
    pub async fn acts_for(&self, run_id: RunId) -> Result<ActsFor, PortError> {
        let mut connection = self.pool().acquire().await.map_err(unavailable)?;
        acts_for_in(&mut connection, run_id).await
    }
}
