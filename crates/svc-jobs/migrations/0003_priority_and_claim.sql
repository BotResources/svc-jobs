-- Jobs 0.4: priorities, claim-based dispatch, per-RunnerType settings.
--
-- Schema only for the facts: no existing row is rewritten. Every column added
-- to an existing table is a plain type whose default is a constant (or none),
-- and its CHECK and foreign-key constraints are added NOT VALID then
-- validated in a separate statement, so the large tables are scanned, never
-- rewritten. Existing Jobs read as requesting and waiting at priority 3,
-- existing RunnerTypes as PUSH following the service default maximum Run
-- duration, and an existing Run without a maximum of its own answers to the
-- service default.
--
-- Two access paths are created empty: `waiting_jobs`, the Jobs waiting for a
-- Run, and `unstarted_pushed_runs`, the triggers published and not yet taken.
-- They hold no business truth of their own — every row derives from the Jobs,
-- Runs, retry schedules and resolutions — and they are maintained in the
-- transaction that records each fact. The service fills and repairs them from
-- those facts at boot and on every backstop sweep, which also covers the work a
-- 0.3 pod still records during a rolling update.

ALTER TABLE jobs
    ADD COLUMN requested_priority smallint NOT NULL DEFAULT 3,
    ADD COLUMN effective_priority smallint NOT NULL DEFAULT 3,
    ADD COLUMN forced_priority smallint,
    ADD COLUMN root_job_id uuid;

ALTER TABLE jobs
    ADD CONSTRAINT jobs_requested_priority_level
        CHECK (requested_priority BETWEEN 1 AND 5) NOT VALID,
    ADD CONSTRAINT jobs_effective_priority_level
        CHECK (effective_priority BETWEEN 1 AND 5) NOT VALID,
    ADD CONSTRAINT jobs_forced_priority_is_effective
        CHECK (forced_priority IS NULL OR forced_priority = effective_priority) NOT VALID,
    ADD CONSTRAINT jobs_root_job_fk
        FOREIGN KEY (root_job_id) REFERENCES jobs(id) NOT VALID;

ALTER TABLE jobs VALIDATE CONSTRAINT jobs_requested_priority_level;
ALTER TABLE jobs VALIDATE CONSTRAINT jobs_effective_priority_level;
ALTER TABLE jobs VALIDATE CONSTRAINT jobs_forced_priority_is_effective;
ALTER TABLE jobs VALIDATE CONSTRAINT jobs_root_job_fk;

CREATE INDEX jobs_triggered_by
    ON jobs (triggered_by_id)
    WHERE triggered_by_id IS NOT NULL;

-- A user's maximum priority. No row: the service default applies. A maximum
-- may be set before Jobs ever observed the user, so the user is not required
-- among known_users.
CREATE TABLE user_max_priorities (
    user_id uuid_v7 PRIMARY KEY,
    max_priority smallint NOT NULL,
    set_by_id uuid_v7 NOT NULL REFERENCES known_users(id),
    set_at timestamptz NOT NULL,
    CHECK (max_priority BETWEEN 1 AND 5)
);

-- NULL maximum Run duration: the RunnerType follows the service default.
-- `dispatch_mode_since`: when the mode last changed; NULL while the type keeps
-- the mode it was registered with.
ALTER TABLE registered_runner_types
    ADD COLUMN dispatch_mode text NOT NULL DEFAULT 'PUSH',
    ADD COLUMN max_run_duration_seconds bigint,
    ADD COLUMN dispatch_mode_since timestamptz;

ALTER TABLE registered_runner_types
    ADD CONSTRAINT registered_runner_types_dispatch_mode
        CHECK (dispatch_mode IN ('PUSH', 'CLAIM')) NOT VALID,
    ADD CONSTRAINT registered_runner_types_max_run_duration_positive
        CHECK (max_run_duration_seconds IS NULL OR max_run_duration_seconds > 0) NOT VALID;

ALTER TABLE registered_runner_types
    VALIDATE CONSTRAINT registered_runner_types_dispatch_mode;
ALTER TABLE registered_runner_types
    VALIDATE CONSTRAINT registered_runner_types_max_run_duration_positive;

-- The maximum duration a Run was dispatched with, from its RunnerType.
ALTER TABLE runs
    ADD COLUMN max_run_duration_seconds bigint;

ALTER TABLE runs
    ADD CONSTRAINT runs_max_run_duration_positive
        CHECK (max_run_duration_seconds IS NULL OR max_run_duration_seconds > 0) NOT VALID;

ALTER TABLE runs VALIDATE CONSTRAINT runs_max_run_duration_positive;

-- A Run withdrawn unstarted — its trigger stranded by a switch to CLAIM — is
-- cancelled without counting as an attempt: the Run that replaces it takes the
-- same attempt number and the same retry schedule. The uniqueness of both
-- holds among the Runs that count.
ALTER TABLE runs
    ADD COLUMN withdrawn_reason text;

ALTER TABLE runs
    ADD CONSTRAINT runs_withdrawn_reason_not_blank
        CHECK (withdrawn_reason IS NULL OR withdrawn_reason <> '') NOT VALID;

ALTER TABLE runs VALIDATE CONSTRAINT runs_withdrawn_reason_not_blank;

CREATE UNIQUE INDEX runs_counted_attempt
    ON runs (job_id, attempt_number)
    WHERE withdrawn_reason IS NULL;

CREATE UNIQUE INDEX runs_counted_retry_schedule
    ON runs (automatic_retry_schedule_id)
    WHERE withdrawn_reason IS NULL AND automatic_retry_schedule_id IS NOT NULL;

ALTER TABLE runs DROP CONSTRAINT runs_job_id_attempt_number_key;
ALTER TABLE runs DROP CONSTRAINT runs_automatic_retry_schedule_id_key;

-- The status of a Job counts the Runs that count.
CREATE OR REPLACE VIEW job_states AS
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
      AND r.withdrawn_reason IS NULL
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
       AND dispatched_retry.withdrawn_reason IS NULL
    WHERE failed_run.job_id = j.id
      AND dispatched_retry.id IS NULL
    ORDER BY rrs.due_at DESC, rrs.id DESC
    LIMIT 1
) next_retry ON true
LEFT JOIN job_deletions jd ON jd.job_id = j.id;

-- A Run created by a claim, reserved to the Instance that claimed it. One
-- claim identity reserves one Run per Instance: a repeated claim finds it, a
-- concurrent repeat on another pod collides here instead of reserving twice.
-- `settled_at` is set when the Run starts or settles, so the lost-claim
-- backstop reads the open reservations alone.
CREATE TABLE run_reservations (
    run_id uuid_v7 PRIMARY KEY REFERENCES runs(id),
    instance_id uuid_v7 NOT NULL REFERENCES runner_instances(id),
    claim_id uuid NOT NULL,
    reserved_at timestamptz NOT NULL,
    settled_at timestamptz,
    UNIQUE (instance_id, claim_id)
);

CREATE INDEX run_reservations_open
    ON run_reservations (reserved_at, run_id)
    WHERE settled_at IS NULL;

-- The Jobs waiting for their next Run: queued and never dispatched
-- (`due_at` NULL), waiting for an automatic retry due at `due_at`, or whose
-- trigger was withdrawn. A row is written with the fact that makes the Job
-- wait and removed with the fact that ends the wait (a dispatch, a
-- reservation, a resolution). `root_job_id` is the root of the Job's tree, or
-- its own id.
CREATE TABLE waiting_jobs (
    job_id uuid_v7 PRIMARY KEY REFERENCES jobs(id),
    runner_type_id uuid_v7 NOT NULL REFERENCES runner_types(id),
    root_job_id uuid NOT NULL,
    effective_priority smallint NOT NULL,
    created_at timestamptz NOT NULL,
    due_at timestamptz,
    CHECK (effective_priority BETWEEN 1 AND 5)
);

-- Dispatch order reads the waiting Jobs of a type root by root, level by
-- level, oldest first: the head of each (root, level) is its first entry, and
-- the turn of every other Job is its position — no sort of the queue.
CREATE INDEX waiting_jobs_heads
    ON waiting_jobs (runner_type_id, root_job_id, effective_priority, created_at, job_id);

CREATE INDEX waiting_jobs_due
    ON waiting_jobs (due_at)
    WHERE due_at IS NOT NULL;

-- Strict round-robin between roots needs to know when each root was last
-- served, per RunnerType: written with every dispatch and reservation.
CREATE TABLE root_dispatch_turns (
    runner_type_id uuid_v7 NOT NULL REFERENCES runner_types(id),
    root_job_id uuid NOT NULL,
    last_served_at timestamptz NOT NULL,
    PRIMARY KEY (runner_type_id, root_job_id)
);

-- The triggers published for PUSH and not yet taken: a Run dispatched without
-- a claim, neither started nor settled. Once its type is claimed from, a row
-- older than the reservation delay after the switch is a stranded trigger.
CREATE TABLE unstarted_pushed_runs (
    run_id uuid_v7 PRIMARY KEY REFERENCES runs(id),
    job_id uuid_v7 NOT NULL REFERENCES jobs(id),
    runner_type_id uuid_v7 NOT NULL REFERENCES runner_types(id),
    dispatched_at timestamptz NOT NULL
);

CREATE INDEX unstarted_pushed_runs_type
    ON unstarted_pushed_runs (runner_type_id, dispatched_at);
