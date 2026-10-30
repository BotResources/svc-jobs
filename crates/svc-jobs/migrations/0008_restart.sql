ALTER TABLE directory_agents
    ADD COLUMN restarted_from_run_id uuid_v7 REFERENCES runs(id);

CREATE INDEX directory_agents_restarted_from
    ON directory_agents (restarted_from_run_id)
    WHERE restarted_from_run_id IS NOT NULL;

CREATE INDEX restart_origins_run
    ON restart_origins (restarts_run_id);

CREATE TABLE restart_wakes (
    run_id uuid_v7 PRIMARY KEY REFERENCES runs(id),
    requested_at timestamptz NOT NULL
);
