CREATE DOMAIN uuid_v7 AS uuid
    CHECK ((get_byte(uuid_send(VALUE), 6) >> 4) = 7);

CREATE DOMAIN job_resolution_kind AS text
    CHECK (VALUE IN ('COMPLETED', 'FAILED', 'CANCELLED'));

CREATE DOMAIN job_failure_cause AS text
    CHECK (VALUE IN (
        'TERMINAL_RUN_FAILURE',
        'DECLARED_BY_OWNER',
        'INACTIVITY_TIMEOUT'
    ));

CREATE DOMAIN run_terminal_kind AS text
    CHECK (VALUE IN ('COMPLETED', 'FAILED', 'CANCELLED'));

CREATE DOMAIN run_failure_kind AS text
    CHECK (VALUE IN ('TRANSIENT', 'PERMANENT'));

CREATE DOMAIN run_log_level AS text
    CHECK (VALUE IN ('INFO', 'WARNING', 'ERROR'));

CREATE TABLE domain_events (
    id uuid_v7 PRIMARY KEY,
    aggregate_id uuid_v7 NOT NULL,
    aggregate_type text NOT NULL,
    aggregate_version bigint NOT NULL,
    event_type text NOT NULL,
    payload jsonb NOT NULL,
    metadata jsonb NOT NULL,
    occurred_at timestamptz NOT NULL,
    UNIQUE (aggregate_type, aggregate_id, aggregate_version),
    CHECK (aggregate_type <> ''),
    CHECK (aggregate_version > 0),
    CHECK (event_type <> '')
);

CREATE INDEX domain_events_aggregate_time
    ON domain_events (
        aggregate_type,
        aggregate_id,
        occurred_at,
        aggregate_version
    );

CREATE INDEX domain_events_type_time
    ON domain_events (event_type, occurred_at, id);

CREATE TABLE integration_outbox (
    id uuid_v7 PRIMARY KEY,
    subject text NOT NULL,
    payload jsonb NOT NULL,
    status text NOT NULL DEFAULT 'PENDING',
    attempts bigint NOT NULL DEFAULT 0,
    last_error text,
    published_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    CHECK (subject <> ''),
    CHECK (status IN ('PENDING', 'PUBLISHED', 'FAILED')),
    CHECK (attempts >= 0),
    CHECK (status <> 'PUBLISHED' OR published_at IS NOT NULL)
);

CREATE INDEX integration_outbox_pending
    ON integration_outbox (id)
    WHERE status = 'PENDING';

CREATE TABLE known_users (
    id uuid_v7 PRIMARY KEY,
    display_name text NOT NULL,
    observed_at timestamptz NOT NULL
);

CREATE TABLE producers (
    id uuid_v7 PRIMARY KEY,
    bc_key text NOT NULL UNIQUE,
    CHECK (bc_key <> '')
);

CREATE TABLE source_entities (
    id uuid_v7 PRIMARY KEY,
    producer_id uuid_v7 NOT NULL REFERENCES producers(id),
    external_id uuid_v7 NOT NULL,
    UNIQUE (producer_id, external_id)
);

CREATE TABLE runner_types (
    id uuid_v7 PRIMARY KEY,
    type_key text NOT NULL UNIQUE,
    CHECK (type_key <> '')
);

CREATE TABLE runner_instances (
    id uuid_v7 PRIMARY KEY,
    runner_type_id uuid_v7 NOT NULL REFERENCES runner_types(id),
    instance_key text NOT NULL,
    UNIQUE (runner_type_id, instance_key),
    CHECK (instance_key <> '')
);

CREATE TABLE runner_presence_sessions (
    id uuid_v7 PRIMARY KEY,
    instance_id uuid_v7 NOT NULL REFERENCES runner_instances(id),
    version text NOT NULL,
    connected_at timestamptz NOT NULL,
    last_observed_at timestamptz NOT NULL,
    disconnected_at timestamptz,
    disconnect_reason_code text,
    CHECK (version <> ''),
    CHECK (last_observed_at >= connected_at),
    CHECK (disconnected_at IS NULL OR disconnected_at >= connected_at),
    CHECK ((disconnected_at IS NULL) = (disconnect_reason_code IS NULL))
);

CREATE UNIQUE INDEX runner_presence_sessions_one_open_per_instance
    ON runner_presence_sessions (instance_id)
    WHERE disconnected_at IS NULL;

CREATE INDEX runner_presence_sessions_instance_time
    ON runner_presence_sessions (instance_id, connected_at DESC);

CREATE TABLE runner_status_changes (
    session_id uuid_v7 NOT NULL REFERENCES runner_presence_sessions(id),
    change_number integer NOT NULL,
    reported_status text NOT NULL,
    capacity integer NOT NULL,
    observed_at timestamptz NOT NULL,
    PRIMARY KEY (session_id, change_number),
    CHECK (change_number > 0),
    CHECK (reported_status <> ''),
    CHECK (capacity >= 1)
);

CREATE TABLE jobs (
    id uuid_v7 PRIMARY KEY,
    runner_type_id uuid_v7 NOT NULL REFERENCES runner_types(id),
    config jsonb,
    parent_job_id uuid_v7 REFERENCES jobs(id),
    predecessor_job_id uuid_v7 UNIQUE REFERENCES jobs(id),
    triggered_by_id uuid_v7 REFERENCES known_users(id),
    producer_id uuid_v7 REFERENCES producers(id),
    source_entity_id uuid_v7 REFERENCES source_entities(id),
    max_attempts integer,
    created_at timestamptz NOT NULL,
    CHECK (parent_job_id IS NULL OR parent_job_id <> id),
    CHECK (predecessor_job_id IS NULL OR predecessor_job_id <> id),
    CHECK (num_nonnulls(producer_id, source_entity_id) = 1),
    CHECK (max_attempts IS NULL OR max_attempts > 0)
);

CREATE INDEX jobs_runner_type_created
    ON jobs (runner_type_id, created_at DESC, id DESC);

CREATE INDEX jobs_parent_created
    ON jobs (parent_job_id, created_at, id)
    WHERE parent_job_id IS NOT NULL;

CREATE INDEX jobs_predecessor
    ON jobs (predecessor_job_id)
    WHERE predecessor_job_id IS NOT NULL;

CREATE INDEX jobs_producer_created
    ON jobs (producer_id, created_at DESC, id DESC)
    WHERE producer_id IS NOT NULL;

CREATE INDEX jobs_source_entity_created
    ON jobs (source_entity_id, created_at DESC, id DESC)
    WHERE source_entity_id IS NOT NULL;

CREATE TABLE job_deletions (
    job_id uuid_v7 PRIMARY KEY REFERENCES jobs(id),
    deleted_by_id uuid_v7 NOT NULL REFERENCES known_users(id),
    deleted_at timestamptz NOT NULL
);

CREATE TABLE runs (
    id uuid_v7 PRIMARY KEY,
    job_id uuid_v7 NOT NULL REFERENCES jobs(id),
    attempt_number integer NOT NULL,
    dispatched_at timestamptz NOT NULL,
    automatic_retry_schedule_id uuid_v7,
    UNIQUE (job_id, attempt_number),
    UNIQUE (id, job_id),
    UNIQUE (automatic_retry_schedule_id),
    CHECK (attempt_number > 0),
    CHECK (
        (attempt_number = 1
            AND automatic_retry_schedule_id IS NULL)
        OR
        (attempt_number > 1
            AND automatic_retry_schedule_id IS NOT NULL)
    )
);

CREATE INDEX runs_job_dispatched
    ON runs (job_id, dispatched_at, id);

CREATE TABLE run_starts (
    run_id uuid_v7 PRIMARY KEY REFERENCES runs(id),
    instance_id uuid_v7 NOT NULL REFERENCES runner_instances(id),
    started_at timestamptz NOT NULL
);

CREATE INDEX run_starts_instance_started
    ON run_starts (instance_id, started_at DESC, run_id);

CREATE TABLE run_terminals (
    run_id uuid_v7 PRIMARY KEY REFERENCES runs(id),
    kind run_terminal_kind NOT NULL,
    occurred_at timestamptz NOT NULL,
    failure_kind run_failure_kind,
    reason_code text,
    params jsonb,
    diagnostic jsonb,
    retry_after_hint interval,
    CHECK (
        (kind = 'FAILED'
            AND failure_kind IS NOT NULL
            AND reason_code IS NOT NULL
            AND reason_code <> ''
            AND params IS NOT NULL
            AND diagnostic IS NOT NULL)
        OR
        (kind <> 'FAILED'
            AND failure_kind IS NULL
            AND reason_code IS NULL
            AND params IS NULL
            AND diagnostic IS NULL
            AND retry_after_hint IS NULL)
    ),
    CHECK (retry_after_hint IS NULL OR retry_after_hint >= interval '0 seconds')
);

CREATE INDEX run_terminals_kind_time
    ON run_terminals (kind, occurred_at DESC, run_id);

CREATE TABLE run_retry_schedules (
    id uuid_v7 PRIMARY KEY,
    failed_run_id uuid_v7 NOT NULL UNIQUE REFERENCES runs(id),
    due_at timestamptz NOT NULL
);

CREATE INDEX run_retry_schedules_due
    ON run_retry_schedules (due_at, id);

CREATE TABLE job_resolutions (
    id uuid_v7 PRIMARY KEY,
    job_id uuid_v7 NOT NULL REFERENCES jobs(id),
    kind job_resolution_kind NOT NULL,
    occurred_at timestamptz NOT NULL,
    failure_cause job_failure_cause,
    caused_by_run_id uuid_v7,
    UNIQUE (id, job_id),
    FOREIGN KEY (caused_by_run_id, job_id)
        REFERENCES runs(id, job_id),
    CHECK (
        (kind = 'FAILED' AND failure_cause IS NOT NULL)
        OR
        (kind <> 'FAILED' AND failure_cause IS NULL)
    ),
    CHECK (
        failure_cause <> 'TERMINAL_RUN_FAILURE'
        OR caused_by_run_id IS NOT NULL
    )
);

CREATE INDEX job_resolutions_job_time
    ON job_resolutions (job_id, occurred_at DESC, id DESC);

CREATE TABLE manual_retries (
    id uuid_v7 PRIMARY KEY,
    failed_resolution_id uuid_v7 NOT NULL UNIQUE,
    predecessor_job_id uuid_v7 NOT NULL REFERENCES jobs(id),
    successor_job_id uuid_v7 NOT NULL UNIQUE REFERENCES jobs(id),
    requested_by_id uuid_v7 NOT NULL REFERENCES known_users(id),
    requested_at timestamptz NOT NULL,
    CHECK (predecessor_job_id <> successor_job_id),
    FOREIGN KEY (failed_resolution_id, predecessor_job_id)
        REFERENCES job_resolutions(id, job_id)
);

CREATE INDEX manual_retries_requested
    ON manual_retries (requested_at, id);

ALTER TABLE runs
    ADD CONSTRAINT runs_automatic_retry_schedule_fk
        FOREIGN KEY (automatic_retry_schedule_id)
        REFERENCES run_retry_schedules(id);

CREATE TABLE run_plan_declarations (
    id uuid_v7 PRIMARY KEY,
    run_id uuid_v7 NOT NULL REFERENCES runs(id),
    declaration_number integer NOT NULL,
    declared_at timestamptz NOT NULL,
    UNIQUE (run_id, declaration_number),
    CHECK (declaration_number > 0)
);

CREATE INDEX run_plan_declarations_current
    ON run_plan_declarations (run_id, declaration_number DESC);

CREATE TABLE run_plan_items (
    declaration_id uuid_v7 NOT NULL REFERENCES run_plan_declarations(id),
    step_index integer NOT NULL,
    label text NOT NULL,
    PRIMARY KEY (declaration_id, step_index),
    CHECK (step_index >= 0),
    CHECK (label <> '')
);

CREATE TABLE steps (
    run_id uuid_v7 NOT NULL REFERENCES runs(id),
    step_index integer NOT NULL,
    label text NOT NULL,
    started_at timestamptz NOT NULL,
    PRIMARY KEY (run_id, step_index),
    CHECK (step_index >= 0),
    CHECK (label <> '')
);

CREATE INDEX steps_run_started
    ON steps (run_id, started_at, step_index);

CREATE TABLE run_logs (
    id uuid_v7 NOT NULL,
    run_id uuid_v7 NOT NULL REFERENCES runs(id),
    step_index integer,
    level run_log_level NOT NULL,
    message text NOT NULL,
    logged_at timestamptz NOT NULL,
    PRIMARY KEY (logged_at, id),
    CHECK (step_index IS NULL OR step_index >= 0)
) PARTITION BY RANGE (logged_at);

CREATE INDEX run_logs_run_time
    ON run_logs (run_id, logged_at, id);

CREATE INDEX run_logs_message_id
    ON run_logs (id);

DO $$
DECLARE
    first_month date := date '2026-01-01';
    months integer := 60;
    lower_bound date;
    upper_bound date;
BEGIN
    FOR month_offset IN 0..months - 1 LOOP
        lower_bound := first_month + (month_offset || ' months')::interval;
        upper_bound := first_month + ((month_offset + 1) || ' months')::interval;
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF run_logs FOR VALUES FROM (%L) TO (%L)',
            'run_logs_' || to_char(lower_bound, 'YYYY_MM'),
            lower_bound,
            upper_bound
        );
    END LOOP;
END $$;

CREATE TABLE run_cancellation_requests (
    run_id uuid_v7 PRIMARY KEY REFERENCES runs(id),
    requested_at timestamptz NOT NULL,
    reason_code text NOT NULL,
    requested_by_id uuid_v7 REFERENCES known_users(id),
    originating_job_id uuid_v7 REFERENCES jobs(id),
    CHECK (reason_code <> '')
);

CREATE INDEX run_cancellation_requests_time
    ON run_cancellation_requests (requested_at DESC, run_id);

CREATE VIEW run_states AS
SELECT
    r.id AS run_id,
    CASE
        WHEN rt.kind IS NOT NULL THEN rt.kind::text
        WHEN rs.run_id IS NOT NULL THEN 'STARTED'
        ELSE 'PENDING'
    END AS status,
    rs.instance_id,
    rs.started_at,
    rt.occurred_at AS finished_at
FROM runs r
LEFT JOIN run_starts rs ON rs.run_id = r.id
LEFT JOIN run_terminals rt ON rt.run_id = r.id;

CREATE VIEW run_progressions AS
SELECT
    r.id AS run_id,
    current_plan.id AS current_plan_declaration_id,
    current_plan.declaration_number,
    CASE
        WHEN current_plan.id IS NULL THEN NULL
        ELSE plan_count.planned_step_count
    END AS planned_step_count,
    current_step.step_index AS current_step_index,
    current_step.label AS current_step_label,
    current_step.started_at AS current_step_started_at
FROM runs r
LEFT JOIN LATERAL (
    SELECT rpd.id, rpd.declaration_number
    FROM run_plan_declarations rpd
    WHERE rpd.run_id = r.id
    ORDER BY rpd.declaration_number DESC
    LIMIT 1
) current_plan ON true
LEFT JOIN LATERAL (
    SELECT count(*)::integer AS planned_step_count
    FROM run_plan_items rpi
    WHERE rpi.declaration_id = current_plan.id
) plan_count ON true
LEFT JOIN LATERAL (
    SELECT s.step_index, s.label, s.started_at
    FROM steps s
    WHERE s.run_id = r.id
    ORDER BY s.step_index DESC, s.started_at DESC
    LIMIT 1
) current_step ON true;

CREATE VIEW job_states AS
SELECT
    j.id AS job_id,
    CASE
        WHEN current_resolution.kind IS NOT NULL
            THEN current_resolution.kind::text
        WHEN run_summary.attempt_count = 0 THEN 'PENDING'
        ELSE 'IN_PROGRESS'
    END AS status,
    run_summary.attempt_count,
    active_run.id AS active_run_id,
    CASE
        WHEN current_resolution.kind IS NULL THEN next_retry.due_at
        ELSE NULL
    END AS next_attempt_at,
    (jd.job_id IS NOT NULL) AS is_deleted
FROM jobs j
LEFT JOIN LATERAL (
    SELECT jr.kind
    FROM job_resolutions jr
    WHERE jr.job_id = j.id
    ORDER BY jr.occurred_at DESC, jr.id DESC
    LIMIT 1
) current_resolution ON true
LEFT JOIN LATERAL (
    SELECT count(*)::integer AS attempt_count
    FROM runs r
    WHERE r.job_id = j.id
) run_summary ON true
LEFT JOIN LATERAL (
    SELECT r.id
    FROM runs r
    LEFT JOIN run_terminals rt ON rt.run_id = r.id
    WHERE r.job_id = j.id
      AND rt.run_id IS NULL
    ORDER BY r.attempt_number DESC
    LIMIT 1
) active_run ON true
LEFT JOIN LATERAL (
    SELECT rrs.due_at
    FROM run_retry_schedules rrs
    JOIN runs failed_run ON failed_run.id = rrs.failed_run_id
    LEFT JOIN runs dispatched_retry
        ON dispatched_retry.automatic_retry_schedule_id = rrs.id
    WHERE failed_run.job_id = j.id
      AND dispatched_retry.id IS NULL
    ORDER BY rrs.due_at DESC, rrs.id DESC
    LIMIT 1
) next_retry ON true
LEFT JOIN job_deletions jd ON jd.job_id = j.id;
