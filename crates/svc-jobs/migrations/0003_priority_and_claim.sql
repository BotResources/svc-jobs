-- Jobs 0.4: priorities, claim-based dispatch, per-RunnerType settings.
--
-- Schema only: every new column has a default or is nullable, and no row is
-- rewritten. Existing Jobs read as requesting and waiting at priority 3,
-- existing RunnerTypes as PUSH with a 72 hour maximum Run duration, and an
-- existing Run without a maximum duration of its own answers to the service
-- default.

CREATE DOMAIN job_priority AS smallint
    CHECK (VALUE BETWEEN 1 AND 5);

CREATE DOMAIN runner_dispatch_mode AS text
    CHECK (VALUE IN ('PUSH', 'CLAIM'));

ALTER TABLE jobs
    ADD COLUMN requested_priority job_priority NOT NULL DEFAULT 3,
    ADD COLUMN effective_priority job_priority NOT NULL DEFAULT 3,
    ADD COLUMN forced_priority job_priority,
    ADD COLUMN root_job_id uuid_v7 REFERENCES jobs(id),
    ADD CONSTRAINT jobs_forced_priority_is_effective
        CHECK (forced_priority IS NULL OR forced_priority = effective_priority);

-- The root a Job belongs to: written at admission. A Job admitted before 0.4
-- has none recorded and is read as belonging to its parent, or to itself.
CREATE INDEX jobs_root
    ON jobs ((coalesce(root_job_id, parent_job_id, id)));

CREATE INDEX jobs_dispatch_order
    ON jobs (runner_type_id, effective_priority DESC, created_at, id);

CREATE INDEX jobs_triggered_by
    ON jobs (triggered_by_id)
    WHERE triggered_by_id IS NOT NULL;

-- A user's maximum priority. No row: the service default applies. A maximum
-- may be set before Jobs ever observed the user, so the user is not required
-- among known_users.
CREATE TABLE user_max_priorities (
    user_id uuid_v7 PRIMARY KEY,
    max_priority job_priority NOT NULL,
    set_by_id uuid_v7 NOT NULL REFERENCES known_users(id),
    set_at timestamptz NOT NULL
);

ALTER TABLE registered_runner_types
    ADD COLUMN dispatch_mode runner_dispatch_mode NOT NULL DEFAULT 'PUSH',
    ADD COLUMN max_run_duration_seconds bigint NOT NULL DEFAULT 259200,
    ADD CONSTRAINT registered_runner_types_max_run_duration_positive
        CHECK (max_run_duration_seconds > 0);

-- The maximum duration a Run was dispatched with, from its RunnerType.
ALTER TABLE runs
    ADD COLUMN max_run_duration_seconds bigint,
    ADD CONSTRAINT runs_max_run_duration_positive
        CHECK (max_run_duration_seconds IS NULL OR max_run_duration_seconds > 0);

-- A Run created by a claim, reserved to the Instance that claimed it. One
-- claim identity reserves one Run per Instance: a repeated claim finds it, a
-- concurrent repeat on another pod collides here instead of reserving twice.
CREATE TABLE run_reservations (
    run_id uuid_v7 PRIMARY KEY REFERENCES runs(id),
    instance_id uuid_v7 NOT NULL REFERENCES runner_instances(id),
    claim_id uuid NOT NULL,
    reserved_at timestamptz NOT NULL,
    UNIQUE (instance_id, claim_id)
);

CREATE INDEX run_reservations_reserved
    ON run_reservations (reserved_at, run_id);
