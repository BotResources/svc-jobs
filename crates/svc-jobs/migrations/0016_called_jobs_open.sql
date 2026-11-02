ALTER TABLE jobs
    ADD COLUMN settled_at timestamptz;

UPDATE jobs j
SET settled_at = coalesce(
    (SELECT jr.occurred_at FROM job_resolutions jr WHERE jr.job_id = j.id),
    (SELECT jd.deleted_at FROM job_deletions jd WHERE jd.job_id = j.id))
WHERE j.requester ? 'Run'
  AND j.call IS NOT NULL
  AND (EXISTS (SELECT 1 FROM job_resolutions jr WHERE jr.job_id = j.id)
       OR EXISTS (SELECT 1 FROM job_deletions jd WHERE jd.job_id = j.id));

CREATE INDEX jobs_called_open
    ON jobs (id)
    WHERE requester ? 'Run' AND call IS NOT NULL AND settled_at IS NULL;
