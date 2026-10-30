ALTER TABLE jobs
    ADD COLUMN parent_run_id uuid_v7 REFERENCES runs(id);

CREATE INDEX jobs_parent_run
    ON jobs (parent_run_id)
    WHERE parent_run_id IS NOT NULL;

UPDATE jobs j
SET parent_run_id = (j.requester ->> 'Run')::uuid
WHERE j.requester ? 'Run'
  AND EXISTS (SELECT 1 FROM runs r WHERE r.id = (j.requester ->> 'Run')::uuid);

CREATE TABLE directory_agents (
    agent_id uuid_v7 PRIMARY KEY,
    run_id uuid_v7 NOT NULL REFERENCES runs(id),
    initial_job_id uuid_v7 NOT NULL REFERENCES jobs(id),
    has_inbox boolean NOT NULL,
    declares_restart boolean NOT NULL,
    state text NOT NULL CHECK (state IN ('RUNNING', 'FINISHED')),
    registered_at timestamptz NOT NULL
);

CREATE INDEX directory_agents_run
    ON directory_agents (run_id);

CREATE INDEX directory_agents_tree
    ON directory_agents (initial_job_id);

CREATE TABLE directory_removed_runs (
    run_id uuid_v7 PRIMARY KEY REFERENCES runs(id),
    removed_at timestamptz NOT NULL
);
