use bc_jobs::domain::ids::{JobId, RunId};
use bc_jobs::ports::PortError;
use sqlx::PgPool;
use uuid::Uuid;

use crate::db::hydrate::unavailable;
use crate::db::jobs::NOT_RESOLVED;

const CALLED_BY_A_RUN: &str = "j.requester ? 'Run' AND j.call IS NOT NULL \
     AND NOT EXISTS (SELECT 1 FROM restart_origins ro WHERE ro.job_id = j.id)";

const STILL_OPEN: &str = "j.requester ? 'Run' AND j.call IS NOT NULL AND j.settled_at IS NULL";

const A_CALLER_RUN_COMPLETED: &str = "EXISTS ( \
        WITH RECURSIVE acting(run_id) AS ( \
            SELECT (j.requester ->> 'Run')::uuid \
          UNION \
            SELECT r.id::uuid FROM acting a \
            JOIN restart_origins ro ON ro.restarts_run_id = a.run_id \
            JOIN runs r ON r.job_id = ro.job_id) \
        SELECT 1 FROM acting a \
        JOIN run_terminals t ON t.run_id = a.run_id AND t.kind = 'COMPLETED')";

const A_RUN_OF_THE_JOB_COMPLETED: &str = "EXISTS (SELECT 1 FROM runs r \
     JOIN run_terminals t ON t.run_id = r.id AND t.kind = 'COMPLETED' WHERE r.job_id = j.id)";

const A_READ_MARKER_EXISTS: &str = "EXISTS (SELECT 1 FROM inbox_items i \
     WHERE i.read_at IS NOT NULL \
       AND ((i.kind = 'NOTICE' AND i.address IS NULL \
             AND i.run_id = (j.requester ->> 'Run')::uuid \
             AND i.notice ->> 'type' = 'RUN_COMPLETED') \
         OR (i.kind = 'MAIL' AND i.address = (j.call ->> 'caller_address')::uuid)))";

const STEMS_CTE: &str = "WITH RECURSIVE stems(job_id) AS ( \
        SELECT unnest($1::uuid[]) \
      UNION \
        SELECT r.job_id::uuid FROM stems s \
        JOIN restart_origins ro ON ro.job_id = s.job_id \
        JOIN runs r ON r.id = ro.restarts_run_id)";

pub async fn candidates_after(
    pool: &PgPool,
    after: Option<JobId>,
    limit: usize,
) -> Result<Vec<JobId>, PortError> {
    let sql = format!(
        "SELECT j.id::uuid FROM jobs j \
         WHERE ($1::uuid IS NULL OR j.id > $1) AND {STILL_OPEN} AND {CALLED_BY_A_RUN} \
           AND {NOT_RESOLVED} \
           AND {A_RUN_OF_THE_JOB_COMPLETED} AND {A_CALLER_RUN_COMPLETED} \
           AND {A_READ_MARKER_EXISTS} \
         ORDER BY j.id LIMIT $2"
    );
    let found: Vec<Uuid> = sqlx::query_scalar(&sql)
        .bind(after.map(|job| job.as_uuid()))
        .bind(i64::try_from(limit).unwrap_or(i64::MAX))
        .fetch_all(pool)
        .await
        .map_err(unavailable)?;
    ids(found)
}

pub async fn called_by_runs(pool: &PgPool, runs: &[RunId]) -> Result<Vec<JobId>, PortError> {
    if runs.is_empty() {
        return Ok(Vec::new());
    }
    let runs: Vec<String> = runs.iter().map(|run| run.as_uuid().to_string()).collect();
    let sql = format!(
        "SELECT j.id::uuid FROM jobs j \
         WHERE j.requester ->> 'Run' = ANY($1) AND {CALLED_BY_A_RUN} AND {NOT_RESOLVED}"
    );
    let found: Vec<Uuid> = sqlx::query_scalar(&sql)
        .bind(runs)
        .fetch_all(pool)
        .await
        .map_err(unavailable)?;
    ids(found)
}

pub async fn called_jobs_stemming_to(
    pool: &PgPool,
    jobs: &[JobId],
) -> Result<Vec<JobId>, PortError> {
    if jobs.is_empty() {
        return Ok(Vec::new());
    }
    let jobs: Vec<Uuid> = jobs.iter().map(|job| job.as_uuid()).collect();
    let sql = format!(
        "{STEMS_CTE} SELECT j.id::uuid FROM stems s JOIN jobs j ON j.id = s.job_id \
         WHERE {CALLED_BY_A_RUN} AND {NOT_RESOLVED}"
    );
    let found: Vec<Uuid> = sqlx::query_scalar(&sql)
        .bind(jobs)
        .fetch_all(pool)
        .await
        .map_err(unavailable)?;
    ids(found)
}

fn ids(found: Vec<Uuid>) -> Result<Vec<JobId>, PortError> {
    found
        .into_iter()
        .map(|id| JobId::new(id).map_err(PortError::from))
        .collect()
}
