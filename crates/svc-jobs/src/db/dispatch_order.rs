//! The order waiting Jobs leave the ledger in, as one access path shared by
//! the push pass and the claim responder.
//!
//! Which Job may leave is the domain's decision (`Job::dispatch_run` /
//! `Job::reserve_run`); in which order the candidates are offered to it is
//! this query: highest effective priority first; within a level, strict
//! round-robin between root Jobs — each root's next Job takes its turn, the
//! root served least recently first, and among roots never served the one
//! whose oldest waiting Job is oldest — then creation time. A Job admitted
//! before roots were recorded belongs to its parent's tree, or is its own root.

use super::jobs::{HAS_LIVE_INSTANCE, NO_ACTIVE_RUN, NOT_RESOLVED, UNCONSUMED_RETRY};

/// The runner types the push pass publishes for: an Instance is live, and the
/// type is not claimed from.
pub fn pushed_scope() -> String {
    format!(
        "{HAS_LIVE_INSTANCE} AND NOT EXISTS (SELECT 1 FROM registered_runner_types reg \
         WHERE reg.runner_type_id = j.runner_type_id AND reg.dispatch_mode = 'CLAIM')"
    )
}

/// One runner type, named by its key in `$2`.
pub const CLAIMED_SCOPE: &str =
    "j.runner_type_id = (SELECT rt.id FROM runner_types rt WHERE rt.type_key = $2)";

pub const ORDER_BY: &str = "ORDER BY o.effective_priority DESC, o.turn, \
     o.last_served ASC NULLS FIRST, o.oldest_waiting, o.created_at, o.id";

const ROOT: &str = "coalesce(j.root_job_id, j.parent_job_id, j.id)";

/// A `WITH … ordered AS (…)` prefix over the Jobs waiting at `$1`, restricted
/// by `scope`. Ends before the final `SELECT`.
pub fn ordered_waiting(scope: &str) -> String {
    format!(
        "WITH waiting AS ( \
            SELECT j.id, j.effective_priority, j.created_at, {ROOT} AS root \
            FROM jobs j \
            WHERE {NOT_RESOLVED} AND {NO_ACTIVE_RUN} AND {scope} \
              AND (NOT EXISTS (SELECT 1 FROM runs r WHERE r.job_id = j.id) \
                   OR EXISTS ({UNCONSUMED_RETRY} AND s.due_at <= $1))), \
         roots AS ( \
            SELECT w.root, min(w.created_at) AS oldest_waiting, \
                (SELECT max(served.dispatched_at) FROM runs served \
                    JOIN jobs sj ON sj.id = served.job_id \
                    WHERE coalesce(sj.root_job_id, sj.parent_job_id, sj.id) = w.root) \
                    AS last_served \
            FROM waiting w GROUP BY w.root), \
         ordered AS ( \
            SELECT w.id, w.effective_priority, w.created_at, ro.last_served, ro.oldest_waiting, \
                row_number() OVER (PARTITION BY w.root, w.effective_priority \
                    ORDER BY w.created_at, w.id) AS turn \
            FROM waiting w JOIN roots ro ON ro.root = w.root)"
    )
}
