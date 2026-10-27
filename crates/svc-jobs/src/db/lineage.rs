//! The root of a Job's tree, as SQL.
//!
//! A Job admitted from 0.4 on stores its root in `jobs.root_job_id`. A Job
//! admitted before stores none, and neither does a child a 0.3 pod admits
//! during a rolling update: their root is found by walking up the parents.
//! The stored column is never rewritten for them — the walk is only taken
//! when it is absent.

/// The root of the Job aliased `alias` in the enclosing query, its own id when
/// it has no parent.
pub fn root_of(alias: &str) -> String {
    format!(
        "coalesce({alias}.root_job_id, \
            (WITH RECURSIVE up(id, parent_job_id) AS ( \
                SELECT a.id, a.parent_job_id FROM jobs a WHERE a.id = {alias}.id \
                UNION ALL \
                SELECT p.id, p.parent_job_id FROM jobs p JOIN up ON p.id = up.parent_job_id) \
             SELECT up.id FROM up WHERE up.parent_job_id IS NULL LIMIT 1), \
            {alias}.id)"
    )
}
