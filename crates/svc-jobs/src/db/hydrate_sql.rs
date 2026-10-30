pub(super) fn jobs_sql() -> String {
    format!(
        "SELECT j.id::uuid AS id, rt.type_key AS runner_type, \
     coalesce(p.bc_key, sp.bc_key) AS producer, j.config, \
     j.parent_job_id::uuid AS parent_job_id, j.predecessor_job_id::uuid AS predecessor_job_id, \
     j.triggered_by_id::uuid AS triggered_by_id, tu.display_name AS triggered_by_name, \
     se.external_id::uuid AS source_entity_id, j.max_attempts, j.created_at, \
     j.requested_priority::smallint AS requested_priority, \
     j.effective_priority::smallint AS effective_priority, \
     j.forced_priority::smallint AS forced_priority, \
     parent_rt.type_key AS parent_runner_type, \
     ({root})::uuid AS root_job_id, j.requester, j.passport, j.payload, j.call, \
     ro.restarts_run_id::uuid AS restart_of_run_id \
     FROM jobs j \
     JOIN runner_types rt ON rt.id = j.runner_type_id \
     LEFT JOIN producers p ON p.id = j.producer_id \
     LEFT JOIN source_entities se ON se.id = j.source_entity_id \
     LEFT JOIN producers sp ON sp.id = se.producer_id \
     LEFT JOIN known_users tu ON tu.id = j.triggered_by_id \
     LEFT JOIN restart_origins ro ON ro.job_id = j.id \
     LEFT JOIN jobs parent ON parent.id = j.parent_job_id \
     LEFT JOIN runner_types parent_rt ON parent_rt.id = parent.runner_type_id \
     WHERE j.id = ANY($1)",
        root = crate::db::lineage::root_of("j"),
    )
}

pub(super) const RUNS_SQL: &str = "SELECT id::uuid AS id, job_id::uuid AS job_id, attempt_number, \
     dispatched_at, automatic_retry_schedule_id::uuid AS automatic_retry_schedule_id, \
     max_run_duration_seconds \
     FROM runs WHERE job_id = ANY($1) ORDER BY job_id, attempt_number, dispatched_at, id";

pub(super) const RESERVATIONS_SQL: &str = "SELECT rr.run_id::uuid AS run_id, rr.reserved_at, rr.claim_id, \
     rt.type_key, ri.instance_key FROM run_reservations rr \
     JOIN runs r ON r.id = rr.run_id \
     JOIN runner_instances ri ON ri.id = rr.instance_id \
     JOIN runner_types rt ON rt.id = ri.runner_type_id \
     WHERE r.job_id = ANY($1)";

pub(super) const STARTS_SQL: &str = "SELECT rs.run_id::uuid AS run_id, rs.started_at, rt.type_key, \
     ri.instance_key FROM run_starts rs \
     JOIN runs r ON r.id = rs.run_id \
     JOIN runner_instances ri ON ri.id = rs.instance_id \
     JOIN runner_types rt ON rt.id = ri.runner_type_id \
     WHERE r.job_id = ANY($1)";

pub(super) const TERMINALS_SQL: &str = "SELECT t.run_id::uuid AS run_id, t.kind::text AS kind, t.occurred_at, \
     t.failure_kind::text AS failure_kind, t.reason_code, t.params, t.diagnostic, \
     EXTRACT(EPOCH FROM t.retry_after_hint)::bigint AS retry_after_seconds, \
     r.withdrawn_reason \
     FROM run_terminals t JOIN runs r ON r.id = t.run_id WHERE r.job_id = ANY($1)";

pub(super) const SCHEDULES_SQL: &str = "SELECT s.id::uuid AS id, s.failed_run_id::uuid AS failed_run_id, s.due_at \
     FROM run_retry_schedules s JOIN runs r ON r.id = s.failed_run_id WHERE r.job_id = ANY($1)";

pub(super) const PLANS_SQL: &str = "SELECT d.id::uuid AS id, d.run_id::uuid AS run_id, d.declaration_number, \
     d.declared_at, i.step_index, i.label \
     FROM run_plan_declarations d \
     JOIN runs r ON r.id = d.run_id \
     JOIN run_plan_items i ON i.declaration_id = d.id \
     WHERE r.job_id = ANY($1) AND d.declaration_number = ( \
        SELECT max(x.declaration_number) FROM run_plan_declarations x WHERE x.run_id = d.run_id) \
     ORDER BY d.run_id, i.step_index";

pub(super) const STEPS_SQL: &str = "SELECT s.run_id::uuid AS run_id, s.step_index, s.label, s.started_at \
     FROM steps s JOIN runs r ON r.id = s.run_id WHERE r.job_id = ANY($1) \
     ORDER BY s.run_id, s.step_index";

pub(super) const CANCELLATIONS_SQL: &str = "SELECT c.run_id::uuid AS run_id, c.requested_at, c.reason_code, \
     c.requested_by_id::uuid AS requested_by_id, u.display_name, \
     c.originating_job_id::uuid AS originating_job_id \
     FROM run_cancellation_requests c JOIN runs r ON r.id = c.run_id \
     LEFT JOIN known_users u ON u.id = c.requested_by_id WHERE r.job_id = ANY($1)";

pub(super) const RESOLUTIONS_SQL: &str = "SELECT DISTINCT ON (job_id) job_id::uuid AS job_id, id::uuid AS id, \
     kind::text AS kind, occurred_at, failure_cause::text AS failure_cause, \
     caused_by_run_id::uuid AS caused_by_run_id \
     FROM job_resolutions WHERE job_id = ANY($1) ORDER BY job_id, occurred_at DESC, id DESC";

pub(super) const DELETIONS_SQL: &str = "SELECT d.job_id::uuid AS job_id, d.deleted_by_id::uuid AS deleted_by_id, \
     u.display_name, d.deleted_at FROM job_deletions d \
     JOIN known_users u ON u.id = d.deleted_by_id WHERE d.job_id = ANY($1)";

pub(super) const MANUAL_RETRIES_SQL: &str = "SELECT m.id::uuid AS id, \
     m.predecessor_job_id::uuid AS predecessor_job_id, \
     m.failed_resolution_id::uuid AS failed_resolution_id, \
     m.successor_job_id::uuid AS successor_job_id, \
     m.requested_by_id::uuid AS requested_by_id, u.display_name, m.requested_at, \
     (settled.id IS NOT NULL) AS successor_settled \
     FROM manual_retries m \
     JOIN known_users u ON u.id = m.requested_by_id \
     LEFT JOIN LATERAL (SELECT jr.id FROM job_resolutions jr \
        WHERE jr.job_id = m.successor_job_id LIMIT 1) settled ON true \
     WHERE m.predecessor_job_id = ANY($1)";
