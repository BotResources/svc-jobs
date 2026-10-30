ALTER TABLE jobs
    ADD COLUMN requester jsonb,
    ADD COLUMN passport jsonb,
    ADD COLUMN payload jsonb,
    ADD COLUMN call jsonb;

ALTER TABLE jobs
    ADD CONSTRAINT jobs_requester_object
        CHECK (requester IS NULL OR jsonb_typeof(requester) = 'object') NOT VALID,
    ADD CONSTRAINT jobs_passport_object
        CHECK (passport IS NULL OR jsonb_typeof(passport) = 'object') NOT VALID,
    ADD CONSTRAINT jobs_payload_object
        CHECK (payload IS NULL OR jsonb_typeof(payload) = 'object') NOT VALID,
    ADD CONSTRAINT jobs_call_object
        CHECK (call IS NULL OR jsonb_typeof(call) = 'object') NOT VALID;

ALTER TABLE jobs VALIDATE CONSTRAINT jobs_requester_object;
ALTER TABLE jobs VALIDATE CONSTRAINT jobs_passport_object;
ALTER TABLE jobs VALIDATE CONSTRAINT jobs_payload_object;
ALTER TABLE jobs VALIDATE CONSTRAINT jobs_call_object;

CREATE INDEX jobs_root
    ON jobs (root_job_id)
    WHERE root_job_id IS NOT NULL;

CREATE INDEX jobs_requester_run
    ON jobs ((requester ->> 'Run'))
    WHERE requester ->> 'Run' IS NOT NULL;
