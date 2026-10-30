pub const PUSHED_TYPES: &str = "SELECT ri.runner_type_id FROM runner_instances ri \
     JOIN runner_presence_sessions ps ON ps.instance_id = ri.id \
         AND ps.disconnected_at IS NULL \
     WHERE NOT EXISTS (SELECT 1 FROM registered_runner_types reg \
         WHERE reg.runner_type_id = ri.runner_type_id AND reg.dispatch_mode = 'CLAIM')";

pub fn pushed_in_order() -> String {
    format!(
        "WITH windows AS ( \
            SELECT rt.id AS runner_type_id, f.free \
            FROM unnest($3::text[], $4::int[]) AS f(type_key, free) \
            JOIN runner_types rt ON rt.type_key = f.type_key), \
         waiting AS ( \
            SELECT w.job_id, w.runner_type_id, w.root_job_id, w.effective_priority, \
                w.created_at, \
                min(w.created_at) OVER (PARTITION BY w.runner_type_id, w.root_job_id) \
                    AS oldest_waiting, \
                row_number() OVER (PARTITION BY w.runner_type_id, w.root_job_id, \
                    w.effective_priority ORDER BY w.created_at, w.job_id) AS turn \
            FROM waiting_jobs w \
            WHERE w.runner_type_id IN (SELECT runner_type_id FROM windows) \
              AND w.runner_type_id IN ({PUSHED_TYPES}) \
              AND (w.due_at IS NULL OR w.due_at <= $1)), \
         ranked AS ( \
            SELECT o.*, turns.last_served_at, \
                row_number() OVER (PARTITION BY o.runner_type_id \
                    ORDER BY o.effective_priority DESC, o.turn, \
                        turns.last_served_at ASC NULLS FIRST, \
                        o.oldest_waiting, o.created_at, o.job_id) AS type_rank \
            FROM waiting o \
            LEFT JOIN root_dispatch_turns turns \
                ON turns.runner_type_id = o.runner_type_id \
                AND turns.root_job_id = o.root_job_id) \
         SELECT r.job_id::uuid AS id FROM ranked r \
         JOIN windows win ON win.runner_type_id = r.runner_type_id \
         WHERE r.type_rank <= win.free \
         ORDER BY r.effective_priority DESC, r.turn, r.last_served_at ASC NULLS FIRST, \
            r.oldest_waiting, r.created_at, r.job_id \
         LIMIT $2"
    )
}

pub const CLAIM_CANDIDATES: i64 = 16;

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
