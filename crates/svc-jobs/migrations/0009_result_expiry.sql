ALTER TABLE job_results
    ADD COLUMN expires_at timestamptz;

UPDATE job_results
    SET expires_at = coalesce(read_at, kept_at) + make_interval(secs => 604800);

ALTER TABLE job_results
    ALTER COLUMN expires_at SET NOT NULL;

CREATE INDEX job_results_expires
    ON job_results (expires_at);
