use bc_jobs::ports::PortError;
use chrono::{DateTime, Utc};
use sqlx::PgConnection;
use uuid::Uuid;

use crate::db::hydrate::unavailable;

pub async fn run_finished(tx: &mut PgConnection, run_id: Uuid) -> Result<(), PortError> {
    sqlx::query("UPDATE directory_agents SET state = 'FINISHED' WHERE run_id = $1")
        .bind(run_id)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
    Ok(())
}

pub async fn remove_run_subtree(
    tx: &mut PgConnection,
    run_id: Uuid,
    at: DateTime<Utc>,
) -> Result<(), PortError> {
    sqlx::query(
        "WITH RECURSIVE tree(run_id) AS ( \
            SELECT $1::uuid \
            UNION \
            SELECT r.id FROM tree t \
            JOIN jobs j ON j.parent_run_id = t.run_id \
            JOIN runs r ON r.job_id = j.id), \
         marked AS ( \
            INSERT INTO directory_removed_runs (run_id, removed_at) \
            SELECT run_id, $2 FROM tree ON CONFLICT (run_id) DO NOTHING) \
         DELETE FROM directory_agents WHERE run_id IN (SELECT run_id FROM tree)",
    )
    .bind(run_id)
    .bind(at)
    .execute(&mut *tx)
    .await
    .map_err(unavailable)?;
    Ok(())
}

pub async fn remove_job_subtree(
    tx: &mut PgConnection,
    job_id: Uuid,
    at: DateTime<Utc>,
) -> Result<(), PortError> {
    sqlx::query(
        "WITH RECURSIVE jobs_below(job_id) AS ( \
            SELECT $1::uuid \
            UNION \
            SELECT j.id FROM jobs_below b JOIN jobs j ON j.parent_job_id = b.job_id), \
         tree(run_id) AS ( \
            SELECT r.id FROM runs r JOIN jobs_below b ON b.job_id = r.job_id), \
         marked AS ( \
            INSERT INTO directory_removed_runs (run_id, removed_at) \
            SELECT run_id, $2 FROM tree ON CONFLICT (run_id) DO NOTHING) \
         DELETE FROM directory_agents WHERE run_id IN (SELECT run_id FROM tree)",
    )
    .bind(job_id)
    .bind(at)
    .execute(&mut *tx)
    .await
    .map_err(unavailable)?;
    Ok(())
}
