use bc_jobs::domain::ids::JobId;
use bc_jobs::domain::policy::TreeFacts;
use bc_jobs::ports::PortError;
use bc_jobs::ports::scrub::DueTree;
use chrono::{DateTime, Utc};
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use crate::db::hydrate::unavailable;

pub fn tree_jobs(root: &str) -> String {
    format!(
        "SELECT t.id FROM jobs t WHERE t.root_job_id = {root} \
         UNION \
         SELECT d.id FROM ( \
            WITH RECURSIVE down(id, root_job_id) AS ( \
                SELECT j.id, j.root_job_id FROM jobs j WHERE j.id = {root} \
                UNION ALL \
                SELECT c.id, c.root_job_id FROM jobs c JOIN down ON c.parent_job_id = down.id) \
            SELECT id FROM down WHERE root_job_id IS NULL) d"
    )
}

pub fn facts_of(tree: &str) -> String {
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
            ) m) AS last_activity"
    )
}

pub const INITIAL_JOB: &str =
    "(r.root_job_id = r.id OR (r.root_job_id IS NULL AND r.parent_job_id IS NULL))";

pub fn due_trees_sql() -> String {
    format!(
        "SELECT r.id::uuid AS initial_job_id, r.created_at FROM jobs r \
         JOIN LATERAL ({facts}) f ON true \
         WHERE r.created_at <= $1 \
           AND {INITIAL_JOB} \
           AND (r.created_at, r.id) > ($3, $4::uuid) \
           AND NOT EXISTS (SELECT 1 FROM tree_scrubs s WHERE s.initial_job_id = r.id) \
           AND f.unresolved_jobs = 0 AND f.live_runs = 0 AND f.last_activity <= $1 \
         ORDER BY r.created_at, r.id LIMIT $2",
        facts = facts_of(&tree_jobs("r.id")),
    )
}

pub fn first_cursor() -> (DateTime<Utc>, Uuid) {
    (DateTime::<Utc>::UNIX_EPOCH, Uuid::nil())
}

pub async fn due_trees(
    pool: &sqlx::PgPool,
    due_before: DateTime<Utc>,
    batch: usize,
    after: Option<DueTree>,
) -> Result<Vec<DueTree>, PortError> {
    let (after_created_at, after_id) = after.map_or_else(first_cursor, |tree| {
        (tree.created_at, tree.initial_job_id.as_uuid())
    });
    let rows = sqlx::query(&due_trees_sql())
        .bind(due_before)
        .bind(i64::try_from(batch).unwrap_or(i64::MAX))
        .bind(after_created_at)
        .bind(after_id)
        .fetch_all(pool)
        .await
        .map_err(unavailable)?;
    rows.iter()
        .map(|row| {
            Ok(DueTree {
                initial_job_id: JobId::new(row.try_get("initial_job_id").map_err(unavailable)?)?,
                created_at: row.try_get("created_at").map_err(unavailable)?,
            })
        })
        .collect()
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

pub async fn facts_of_tree(
    tx: &mut PgConnection,
    initial_job_id: JobId,
    jobs: &[Uuid],
) -> Result<TreeFacts, PortError> {
    let row = sqlx::query(&facts_of("SELECT unnest($1::uuid[]) AS id"))
        .bind(jobs)
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
        last_activity: row
            .try_get::<Option<DateTime<Utc>>, _>("last_activity")
            .map_err(unavailable)?
            .unwrap_or(DateTime::<Utc>::MAX_UTC),
        unresolved_jobs: u64::try_from(row.get::<i64, _>("unresolved_jobs")).unwrap_or(u64::MAX),
        live_runs: u64::try_from(row.get::<i64, _>("live_runs")).unwrap_or(u64::MAX),
        scrubbed,
    })
}

pub async fn run_ids_of(tx: &mut PgConnection, jobs: &[Uuid]) -> Result<Vec<Uuid>, PortError> {
    sqlx::query_scalar("SELECT id::uuid FROM runs WHERE job_id = ANY($1)")
        .bind(jobs)
        .fetch_all(&mut *tx)
        .await
        .map_err(unavailable)
}
