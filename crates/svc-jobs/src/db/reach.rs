use bc_jobs::domain::ids::JobId;
use bc_jobs::domain::job::Job;
use bc_jobs::ports::PortError;
use sqlx::PgConnection;
use uuid::Uuid;

use super::PgStore;
use super::hydrate::unavailable;

pub const REACH_CTE: &str = "\
    reach(kind, id) AS ( \
        SELECT 'job', s FROM unnest($1::uuid[]) AS s \
      UNION \
        SELECT n.kind, n.id FROM reach x CROSS JOIN LATERAL ( \
            SELECT 'job' AS kind, j.id::uuid AS id FROM jobs j \
                WHERE x.kind = 'job' AND j.parent_job_id = x.id \
          UNION ALL \
            SELECT 'job', j.id::uuid FROM jobs j \
                WHERE x.kind = 'job' AND j.predecessor_job_id = x.id \
          UNION ALL \
            SELECT 'job', ro.job_id::uuid FROM runs r \
                JOIN restart_origins ro ON ro.restarts_run_id = r.id \
                WHERE x.kind = 'job' AND r.job_id = x.id \
          UNION ALL \
            SELECT 'run', ro.restarts_run_id::uuid FROM restart_origins ro \
                WHERE x.kind = 'job' AND ro.job_id = x.id \
          UNION ALL \
            SELECT 'run', ro.restarts_run_id::uuid FROM runs r \
                JOIN restart_origins ro ON ro.job_id = r.job_id \
                WHERE x.kind = 'run' AND r.id = x.id \
          UNION ALL \
            SELECT 'job', j.id::uuid FROM jobs j \
                WHERE x.kind = 'run' AND j.requester ->> 'Run' = x.id::text \
        ) n)";

impl PgStore {
    pub async fn cancel_reach_in(
        tx: &mut PgConnection,
        roots: &[Uuid],
    ) -> Result<Vec<Uuid>, PortError> {
        if roots.is_empty() {
            return Ok(vec![]);
        }
        let sql = format!(
            "WITH RECURSIVE {REACH_CTE} \
             SELECT DISTINCT id FROM reach WHERE kind = 'job' AND id <> ALL($1)"
        );
        sqlx::query_scalar(&sql)
            .bind(roots)
            .fetch_all(&mut *tx)
            .await
            .map_err(unavailable)
    }

    pub async fn load_cancel_reach(&self, root: JobId) -> Result<Vec<Job>, PortError> {
        let mut connection = self.pool().acquire().await.map_err(unavailable)?;
        let ids = Self::cancel_reach_in(&mut connection, &[root.as_uuid()]).await?;
        self.load_batch(&ids).await
    }
}
