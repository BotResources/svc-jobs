use bc_jobs::ports::PortError;
use chrono::{DateTime, Utc};
use sqlx::PgConnection;
use uuid::Uuid;

use crate::db::hydrate::unavailable;

pub async fn run_finished(
    tx: &mut PgConnection,
    run_id: Uuid,
    at: DateTime<Utc>,
) -> Result<(), PortError> {
    sqlx::query(
        "UPDATE directory_agents SET state = 'FINISHED', restarted_from_run_id = NULL \
         WHERE run_id = $1",
    )
    .bind(run_id)
    .execute(&mut *tx)
    .await
    .map_err(unavailable)?;
    sqlx::query(
        "DELETE FROM saved_states WHERE EXISTS (SELECT 1 FROM saved_states own WHERE own.run_id = $1) \
           AND run_id IN (WITH RECURSIVE chain(run_id) AS ( \
                SELECT ro.restarts_run_id FROM runs r \
                JOIN restart_origins ro ON ro.job_id = r.job_id WHERE r.id = $1 \
              UNION \
                SELECT ro.restarts_run_id FROM chain c \
                JOIN runs r ON r.id = c.run_id \
                JOIN restart_origins ro ON ro.job_id = r.job_id) \
            SELECT run_id FROM chain)",
    )
    .bind(run_id)
    .execute(&mut *tx)
    .await
    .map_err(unavailable)?;
    crate::db::restart::wake_run(tx, run_id, at).await
}

pub async fn run_failed(
    tx: &mut PgConnection,
    run_id: Uuid,
    at: DateTime<Utc>,
) -> Result<(), PortError> {
    sqlx::query(
        "UPDATE directory_agents SET run_id = restarted_from_run_id, state = 'FINISHED', \
            restarted_from_run_id = NULL \
         WHERE run_id = $1 AND restarted_from_run_id IS NOT NULL",
    )
    .bind(run_id)
    .execute(&mut *tx)
    .await
    .map_err(unavailable)?;
    remove_run_subtree(tx, run_id, at).await
}

const REMOVE_TREE: &str = "\
    marked AS ( \
        INSERT INTO directory_removed_runs (run_id, removed_at) \
        SELECT t.run_id, $2 FROM tree t \
        WHERE NOT EXISTS (SELECT 1 FROM run_terminals rt WHERE rt.run_id = t.run_id) \
        ON CONFLICT (run_id) DO NOTHING), \
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
        "WITH RECURSIVE {reach}, \
         tree(run_id) AS ( \
            SELECT r.id FROM runs r WHERE r.job_id IN (SELECT id FROM reach WHERE kind = 'job')), \
         {REMOVE_TREE}",
        reach = crate::db::reach::REACH_CTE
    );
    sqlx::query(&sql)
        .bind(vec![job_id])
        .bind(at)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
    Ok(())
}
