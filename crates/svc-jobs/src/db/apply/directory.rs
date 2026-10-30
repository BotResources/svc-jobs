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

const REMOVE_TREE: &str = "\
    marked AS ( \
        INSERT INTO directory_removed_runs (run_id, removed_at) \
        SELECT run_id, $2 FROM tree ON CONFLICT (run_id) DO NOTHING), \
     tombstones AS ( \
        INSERT INTO directory_removed_addresses (agent_id, removed_at) \
        SELECT agent_id, $2 FROM directory_agents \
        WHERE run_id IN (SELECT run_id FROM tree) \
        ON CONFLICT (agent_id) DO NOTHING), \
     notices AS ( \
        DELETE FROM inbox_items \
        WHERE kind = 'NOTICE' AND run_id IN (SELECT run_id FROM tree)), \
     states AS ( \
        DELETE FROM saved_states WHERE run_id IN (SELECT run_id FROM tree)) \
     DELETE FROM directory_agents WHERE run_id IN (SELECT run_id FROM tree)";

pub async fn remove_run_subtree(
    tx: &mut PgConnection,
    run_id: Uuid,
    at: DateTime<Utc>,
) -> Result<(), PortError> {
    let sql = format!(
        "WITH RECURSIVE tree(run_id) AS ( \
            SELECT $1::uuid \
            UNION \
            SELECT r.id FROM tree t \
            JOIN jobs j ON j.parent_run_id = t.run_id \
            JOIN runs r ON r.job_id = j.id), {REMOVE_TREE}"
    );
    sqlx::query(&sql)
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
    let sql = format!(
        "WITH RECURSIVE jobs_below(job_id) AS ( \
            SELECT $1::uuid \
            UNION \
            SELECT j.id FROM jobs_below b JOIN jobs j ON j.parent_job_id = b.job_id), \
         tree(run_id) AS ( \
            SELECT r.id FROM runs r JOIN jobs_below b ON b.job_id = r.job_id), {REMOVE_TREE}"
    );
    sqlx::query(&sql)
        .bind(job_id)
        .bind(at)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
    Ok(())
}
