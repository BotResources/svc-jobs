//! The order waiting Jobs leave the ledger in, as one access path shared by
//! the push pass and the claim responder.
//!
//! Which Job may leave is the domain's decision (`Job::dispatch_run` /
//! `Job::reserve_run`); in which order the candidates are offered to it is
//! this query over `waiting_jobs` — the indexed set of the Jobs waiting now,
//! maintained with every fact, so a selection never scans the ledger's
//! history. Per RunnerType: highest effective priority first; within a level,
//! strict round-robin between root Jobs — each root's next Job takes its turn,
//! the root served least recently by this type first, and among roots never
//! served the one whose oldest waiting Job is oldest — then creation time.

/// The runner types the push pass publishes for: an Instance is live, and the
/// type is not claimed from.
pub const PUSHED_TYPES: &str = "SELECT ri.runner_type_id FROM runner_instances ri \
     JOIN runner_presence_sessions ps ON ps.instance_id = ri.id \
         AND ps.disconnected_at IS NULL \
     WHERE NOT EXISTS (SELECT 1 FROM registered_runner_types reg \
         WHERE reg.runner_type_id = ri.runner_type_id AND reg.dispatch_mode = 'CLAIM')";

/// One runner type, named by its key in `$2`.
pub const CLAIMED_TYPE: &str = "SELECT rt.id FROM runner_types rt WHERE rt.type_key = $2";

pub const ORDER_BY: &str = "ORDER BY o.effective_priority DESC, o.turn, \
     o.last_served ASC NULLS FIRST, o.oldest_waiting, o.created_at, o.job_id";

/// A `WITH … ordered AS (…)` prefix over the Jobs waiting at `$1` whose
/// runner type is among `types` (a sub-select). Ends before the final
/// `SELECT`.
pub fn ordered_waiting(types: &str) -> String {
    format!(
        "WITH waiting AS ( \
            SELECT w.job_id, w.runner_type_id, w.root_job_id, w.effective_priority, \
                w.created_at \
            FROM waiting_jobs w \
            WHERE w.runner_type_id IN ({types}) \
              AND (w.due_at IS NULL OR w.due_at <= $1)), \
         ordered AS ( \
            SELECT w.job_id, w.effective_priority, w.created_at, turns.last_served_at \
                AS last_served, \
                min(w.created_at) OVER (PARTITION BY w.runner_type_id, w.root_job_id) \
                    AS oldest_waiting, \
                row_number() OVER (PARTITION BY w.runner_type_id, w.root_job_id, \
                    w.effective_priority ORDER BY w.created_at, w.job_id) AS turn \
            FROM waiting w \
            LEFT JOIN root_dispatch_turns turns \
                ON turns.runner_type_id = w.runner_type_id \
               AND turns.root_job_id = w.root_job_id)"
    )
}
