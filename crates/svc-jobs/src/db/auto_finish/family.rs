use sqlx::PgConnection;
use uuid::Uuid;

use crate::db::hydrate::unavailable;
use bc_jobs::ports::PortError;

const RESTART_JOBS_SQL: &str = "WITH RECURSIVE family(job_id) AS ( \
        SELECT $1::uuid \
      UNION \
        SELECT ro.job_id::uuid FROM family f \
        JOIN runs r ON r.job_id = f.job_id \
        JOIN restart_origins ro ON ro.restarts_run_id = r.id) \
     SELECT job_id FROM family WHERE job_id <> $1 ORDER BY job_id";

const PENDING_WAKES_SQL: &str = "SELECT w.run_id::uuid FROM restart_wakes w \
     JOIN runs r ON r.id = w.run_id \
     WHERE r.job_id = ANY($1) ORDER BY w.run_id";

const RUNS_SQL: &str = "SELECT id::uuid FROM runs WHERE job_id = ANY($1)";

pub struct Family {
    pub restart_jobs: Vec<Uuid>,
    pub pending_wakes: Vec<Uuid>,
}

async fn restart_jobs_of(tx: &mut PgConnection, root: Uuid) -> Result<Vec<Uuid>, PortError> {
    sqlx::query_scalar(RESTART_JOBS_SQL)
        .bind(root)
        .fetch_all(&mut *tx)
        .await
        .map_err(unavailable)
}

async fn pending_wakes_of(tx: &mut PgConnection, jobs: &[Uuid]) -> Result<Vec<Uuid>, PortError> {
    sqlx::query_scalar(PENDING_WAKES_SQL)
        .bind(jobs)
        .fetch_all(&mut *tx)
        .await
        .map_err(unavailable)
}

pub async fn runs_of(tx: &mut PgConnection, jobs: &[Uuid]) -> Result<Vec<Uuid>, PortError> {
    sqlx::query_scalar(RUNS_SQL)
        .bind(jobs)
        .fetch_all(&mut *tx)
        .await
        .map_err(unavailable)
}

pub async fn family_of(tx: &mut PgConnection, root: Uuid) -> Result<Family, PortError> {
    let restart_jobs = restart_jobs_of(tx, root).await?;
    let mut members = restart_jobs.clone();
    members.push(root);
    let pending_wakes = pending_wakes_of(tx, &members).await?;
    Ok(Family {
        restart_jobs,
        pending_wakes,
    })
}
