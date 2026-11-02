CREATE TABLE tree_scrubs (
    initial_job_id uuid_v7 PRIMARY KEY REFERENCES jobs(id),
    scrubbed_at timestamptz NOT NULL
);

CREATE INDEX jobs_without_root
    ON jobs (id)
    WHERE root_job_id IS NULL;

CREATE INDEX jobs_initial_created
    ON jobs (created_at, id)
    WHERE root_job_id = id OR (root_job_id IS NULL AND parent_job_id IS NULL);

CREATE INDEX integration_outbox_published_job
    ON integration_outbox ((payload -> 'payload' ->> 'job_id'))
    WHERE status = 'PUBLISHED';

CREATE INDEX mails_initial_job
    ON mails (initial_job_id);

CREATE INDEX job_results_job
    ON job_results (job_id);

CREATE INDEX inbox_items_run_all
    ON inbox_items (run_id)
    WHERE run_id IS NOT NULL;

CREATE INDEX inbox_items_mail_all
    ON inbox_items (mail_id)
    WHERE mail_id IS NOT NULL;

CREATE INDEX inbox_items_address_all
    ON inbox_items (address)
    WHERE address IS NOT NULL;

CREATE INDEX kept_payloads_uploaded
    ON kept_payloads (uploaded_at);

CREATE INDEX mail_attachments_uploaded
    ON mail_attachments (uploaded_at);
