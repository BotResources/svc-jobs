CREATE TABLE current_runs (
    run_id uuid_v7 PRIMARY KEY REFERENCES runs(id),
    job_id uuid_v7 NOT NULL REFERENCES jobs(id),
    runner_type_id uuid_v7 NOT NULL REFERENCES runner_types(id)
);

CREATE INDEX current_runs_runner_type
    ON current_runs (runner_type_id);

CREATE INDEX current_runs_job
    ON current_runs (job_id);

INSERT INTO current_runs (run_id, job_id, runner_type_id)
SELECT r.id, r.job_id, j.runner_type_id
FROM runs r
JOIN jobs j ON j.id = r.job_id
WHERE NOT EXISTS (SELECT 1 FROM run_terminals t WHERE t.run_id = r.id);
