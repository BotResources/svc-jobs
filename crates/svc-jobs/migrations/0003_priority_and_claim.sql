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
-- One access path is filled from the existing facts: `waiting_jobs`, the
-- index of the Jobs waiting for a Run. It holds no business truth of its own —
-- every row is derived from the Jobs, Runs, retry schedules and resolutions
-- already stored, and from then on it is maintained in the transaction that
-- records each fact.

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
ALTER TABLE registered_runner_types
    ADD COLUMN dispatch_mode text NOT NULL DEFAULT 'PUSH',
    ADD COLUMN max_run_duration_seconds bigint;

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
-- (`due_at` NULL), or waiting for an automatic retry due at `due_at`. A row
-- is written with the fact that makes the Job wait and removed with the fact
-- that ends the wait (a dispatch, a reservation, a resolution). `root_job_id`
-- is the Job's root, or its own id.
CREATE TABLE waiting_jobs (
    job_id uuid_v7 PRIMARY KEY REFERENCES jobs(id),
    runner_type_id uuid_v7 NOT NULL REFERENCES runner_types(id),
    root_job_id uuid NOT NULL,
    effective_priority smallint NOT NULL,
    created_at timestamptz NOT NULL,
    due_at timestamptz,
    CHECK (effective_priority BETWEEN 1 AND 5)
);

CREATE INDEX waiting_jobs_dispatch_order
    ON waiting_jobs (runner_type_id, effective_priority DESC, created_at, job_id);

CREATE INDEX waiting_jobs_root
    ON waiting_jobs (runner_type_id, root_job_id);

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

INSERT INTO waiting_jobs (job_id, runner_type_id, root_job_id, effective_priority, created_at, due_at)
SELECT j.id, j.runner_type_id, coalesce(j.parent_job_id, j.id), 3, j.created_at, retry.due_at
FROM jobs j
LEFT JOIN LATERAL (
    SELECT s.due_at
    FROM run_retry_schedules s
    JOIN runs failed ON failed.id = s.failed_run_id
    LEFT JOIN runs consumed ON consumed.automatic_retry_schedule_id = s.id
    WHERE failed.job_id = j.id AND consumed.id IS NULL
    ORDER BY s.due_at DESC
    LIMIT 1
) retry ON true
WHERE NOT EXISTS (SELECT 1 FROM job_resolutions r WHERE r.job_id = j.id)
  AND NOT EXISTS (SELECT 1 FROM job_deletions d WHERE d.job_id = j.id)
  AND NOT EXISTS (
      SELECT 1 FROM runs r LEFT JOIN run_terminals t ON t.run_id = r.id
      WHERE r.job_id = j.id AND t.run_id IS NULL)
  AND (NOT EXISTS (SELECT 1 FROM runs r WHERE r.job_id = j.id) OR retry.due_at IS NOT NULL);
