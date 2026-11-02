use bc_jobs::domain::ids::JobId;
use bc_jobs::domain::policy::TreeFacts;
use bc_jobs::ports::PortError;
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use crate::db::hydrate::unavailable;
use crate::db::lineage::root_of;

pub fn tree_jobs(root: &str) -> String {
    format!(
        "SELECT t.id FROM jobs t WHERE t.root_job_id = {root} \
         UNION \
         SELECT t.id FROM jobs t WHERE t.root_job_id IS NULL AND ({walk}) = {root}",
        walk = root_of("t"),
    )
}

pub fn facts_of(root: &str) -> String {
    format!(
        "WITH tree AS MATERIALIZED ({tree}) \
         SELECT \
            (SELECT count(*) FROM tree tj WHERE NOT EXISTS \
                (SELECT 1 FROM job_resolutions jr WHERE jr.job_id = tj.id)) AS unresolved_jobs, \
            (SELECT count(*) FROM runs lr JOIN tree tj ON tj.id = lr.job_id WHERE NOT EXISTS \
                (SELECT 1 FROM run_terminals lt WHERE lt.run_id = lr.id)) AS live_runs, \
            (SELECT max(m.moment) FROM ( \
                SELECT j.created_at AS moment FROM jobs j JOIN tree tj ON tj.id = j.id \
                UNION ALL \
                SELECT r.dispatched_at FROM runs r JOIN tree tj ON tj.id = r.job_id \
                UNION ALL \
                SELECT rs.started_at FROM run_starts rs JOIN runs r ON r.id = rs.run_id \
                    JOIN tree tj ON tj.id = r.job_id \
                UNION ALL \
                SELECT rt.occurred_at FROM run_terminals rt JOIN runs r ON r.id = rt.run_id \
                    JOIN tree tj ON tj.id = r.job_id \
                UNION ALL \
                SELECT e.occurred_at FROM domain_events e JOIN tree tj ON tj.id = e.aggregate_id \
                    WHERE e.aggregate_type = 'Job' AND e.event_type = 'RunStepStarted' \
                UNION ALL \
                SELECT jr.occurred_at FROM job_resolutions jr JOIN tree tj ON tj.id = jr.job_id \
            ) m) AS last_activity",
        tree = tree_jobs(root),
    )
}

pub fn due_trees_sql() -> String {
    format!(
        "SELECT r.id::uuid AS initial_job_id FROM jobs r \
         JOIN LATERAL ({facts}) f ON true \
         WHERE r.created_at <= $1 \
           AND (r.root_job_id = r.id OR (r.root_job_id IS NULL AND r.parent_job_id IS NULL)) \
           AND NOT EXISTS (SELECT 1 FROM tree_scrubs s WHERE s.initial_job_id = r.id) \
           AND f.unresolved_jobs = 0 AND f.live_runs = 0 AND f.last_activity <= $1 \
         ORDER BY r.created_at, r.id LIMIT $2",
        facts = facts_of("r.id"),
    )
}

pub async fn facts_of_tree(
    tx: &mut PgConnection,
    initial_job_id: JobId,
) -> Result<TreeFacts, PortError> {
    let row = sqlx::query(&facts_of("$1::uuid"))
        .bind(initial_job_id.as_uuid())
        .fetch_one(&mut *tx)
        .await
        .map_err(unavailable)?;
    let scrubbed: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM tree_scrubs WHERE initial_job_id = $1)")
            .bind(initial_job_id.as_uuid())
            .fetch_one(&mut *tx)
            .await
            .map_err(unavailable)?;
    Ok(TreeFacts {
        last_activity: row.get("last_activity"),
        unresolved_jobs: u64::try_from(row.get::<i64, _>("unresolved_jobs")).unwrap_or(u64::MAX),
        live_runs: u64::try_from(row.get::<i64, _>("live_runs")).unwrap_or(u64::MAX),
        scrubbed,
    })
}

pub async fn job_ids_of_tree(
    tx: &mut PgConnection,
    initial_job_id: JobId,
) -> Result<Vec<Uuid>, PortError> {
    let sql = format!("SELECT id::uuid FROM ({}) tree", tree_jobs("$1::uuid"));
    sqlx::query_scalar(&sql)
        .bind(initial_job_id.as_uuid())
        .fetch_all(&mut *tx)
        .await
        .map_err(unavailable)
}

pub async fn run_ids_of(tx: &mut PgConnection, jobs: &[Uuid]) -> Result<Vec<Uuid>, PortError> {
    sqlx::query_scalar("SELECT id::uuid FROM runs WHERE job_id = ANY($1)")
        .bind(jobs)
        .fetch_all(&mut *tx)
        .await
        .map_err(unavailable)
}
