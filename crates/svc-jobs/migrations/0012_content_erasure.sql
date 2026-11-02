ALTER TABLE jobs
    ADD COLUMN content_erased_at timestamptz;

ALTER TABLE mails
    ADD COLUMN content_erased_at timestamptz;

CREATE INDEX mails_sender_address
    ON mails (sender_address);

CREATE TABLE cancelled_request_data (
    job_id uuid_v7 PRIMARY KEY REFERENCES jobs(id)
);

INSERT INTO cancelled_request_data (job_id)
    SELECT job_id FROM job_resolutions WHERE kind = 'CANCELLED';
