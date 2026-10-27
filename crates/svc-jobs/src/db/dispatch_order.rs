//! The order waiting Jobs leave the ledger in, read from `waiting_jobs` alone.
//!
//! Which Job may leave is the domain's decision (`Job::dispatch_run` /
//! `Job::reserve_run`); in which order the candidates are offered to it is
//! decided here. Per RunnerType: highest effective priority first; within a
//! level, strict round-robin between root Jobs — each root's next Job takes
//! its turn, the root served least recently by this type first, and among
//! roots never served the one whose oldest waiting Job is oldest — then
//! creation time.
//!
//! Both selections walk the `waiting_jobs_heads` index — by type, root,
//! level, then age — so the turn of a Job inside its (root, level) is its
//! position in the index, never a sort of the queue, and neither joins the
//! ledger.

/// The runner types the push pass publishes for: an Instance is live, and the
/// type is not claimed from.
pub const PUSHED_TYPES: &str = "SELECT ri.runner_type_id FROM runner_instances ri \
     JOIN runner_presence_sessions ps ON ps.instance_id = ri.id \
         AND ps.disconnected_at IS NULL \
     WHERE NOT EXISTS (SELECT 1 FROM registered_runner_types reg \
         WHERE reg.runner_type_id = ri.runner_type_id AND reg.dispatch_mode = 'CLAIM')";

/// The next `$2` Jobs the push pass offers, waiting at `$1`, in dispatch order.
pub fn pushed_in_order() -> String {
    format!(
        "WITH waiting AS ( \
            SELECT w.job_id, w.runner_type_id, w.root_job_id, w.effective_priority, \
                w.created_at, \
                min(w.created_at) OVER (PARTITION BY w.runner_type_id, w.root_job_id) \
                    AS oldest_waiting, \
                row_number() OVER (PARTITION BY w.runner_type_id, w.root_job_id, \
                    w.effective_priority ORDER BY w.created_at, w.job_id) AS turn \
            FROM waiting_jobs w \
            WHERE w.runner_type_id IN ({PUSHED_TYPES}) \
              AND (w.due_at IS NULL OR w.due_at <= $1)) \
         SELECT o.job_id::uuid AS id FROM waiting o \
         LEFT JOIN root_dispatch_turns turns \
            ON turns.runner_type_id = o.runner_type_id AND turns.root_job_id = o.root_job_id \
         ORDER BY o.effective_priority DESC, o.turn, turns.last_served_at ASC NULLS FIRST, \
            o.oldest_waiting, o.created_at, o.job_id \
         LIMIT $2"
    )
}

/// How many heads a claim considers before locking one: the first heads in
/// dispatch order, so a claim skipping the rows other claims hold still finds
/// one to take.
pub const CLAIM_CANDIDATES: i64 = 16;

/// The next Job of the type keyed `$2` waiting at `$1`, its `waiting_jobs` row
/// locked for this transaction, skipping the rows another claim holds. Only
/// the head of each (root, level) — the Job whose turn it is — competes: one
/// row per pair, ordered by level, the root's last service and the age of its
/// oldest waiting Job. `$3` are the Jobs this claim already passed over.
pub const NEXT_CLAIMABLE: &str = "WITH t AS (SELECT rt.id FROM runner_types rt WHERE rt.type_key = $2), \
     heads AS ( \
        SELECT DISTINCT ON (w.root_job_id, w.effective_priority) \
            w.job_id, w.root_job_id, w.effective_priority, w.created_at \
        FROM waiting_jobs w, t \
        WHERE w.runner_type_id = t.id AND (w.due_at IS NULL OR w.due_at <= $1) \
          AND NOT (w.job_id = ANY($3)) \
        ORDER BY w.root_job_id, w.effective_priority, w.created_at, w.job_id), \
     ranked AS ( \
        SELECT h.job_id, h.effective_priority, h.created_at, \
            min(h.created_at) OVER (PARTITION BY h.root_job_id) AS oldest_waiting, \
            turns.last_served_at \
        FROM heads h CROSS JOIN t \
        LEFT JOIN root_dispatch_turns turns \
            ON turns.runner_type_id = t.id AND turns.root_job_id = h.root_job_id), \
     best AS ( \
        SELECT r.job_id, row_number() OVER ( \
            ORDER BY r.effective_priority DESC, r.last_served_at ASC NULLS FIRST, \
                r.oldest_waiting, r.created_at, r.job_id) AS position \
        FROM ranked r \
        ORDER BY position LIMIT $4) \
     SELECT w.job_id::uuid AS id FROM waiting_jobs w JOIN best b ON b.job_id = w.job_id \
     ORDER BY b.position \
     FOR UPDATE OF w SKIP LOCKED LIMIT 1";
