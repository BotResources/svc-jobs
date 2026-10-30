ALTER TABLE directory_agents
    ALTER COLUMN agent_id TYPE uuid;

CREATE TABLE directory_removed_addresses (
    agent_id uuid PRIMARY KEY,
    removed_at timestamptz NOT NULL
);

CREATE TABLE mails (
    mail_id uuid PRIMARY KEY,
    sender_address uuid NOT NULL,
    sender_run_id uuid_v7 NOT NULL REFERENCES runs(id),
    initial_job_id uuid_v7 NOT NULL REFERENCES jobs(id),
    author text NOT NULL,
    source_conversation_id uuid NOT NULL,
    subject text NOT NULL,
    body text NOT NULL,
    created_at timestamptz NOT NULL
);

CREATE TABLE mail_recipients (
    mail_id uuid NOT NULL REFERENCES mails(mail_id),
    address uuid NOT NULL,
    kind text NOT NULL CHECK (kind IN ('DIRECT', 'CC')),
    PRIMARY KEY (mail_id, address)
);

CREATE INDEX mail_recipients_address
    ON mail_recipients (address);

CREATE TABLE mail_attachments (
    mail_id uuid NOT NULL,
    attachment_id uuid NOT NULL,
    uploader_address uuid NOT NULL,
    name text NOT NULL,
    content_type text NOT NULL,
    content text NOT NULL,
    size bigint NOT NULL CHECK (size >= 0),
    uploaded_at timestamptz NOT NULL,
    listed boolean NOT NULL DEFAULT false,
    PRIMARY KEY (mail_id, attachment_id)
);

CREATE TABLE inbox_items (
    item_id uuid_v7 PRIMARY KEY,
    seq bigint GENERATED ALWAYS AS IDENTITY,
    kind text NOT NULL CHECK (kind IN ('MAIL', 'CC_NOTICE', 'NOTICE')),
    address uuid REFERENCES directory_agents(agent_id) ON DELETE CASCADE,
    run_id uuid_v7 REFERENCES runs(id),
    mail_id uuid REFERENCES mails(mail_id),
    notice jsonb,
    accepted_at timestamptz NOT NULL,
    read_at timestamptz,
    CONSTRAINT inbox_items_mail_shape CHECK (
        (kind IN ('MAIL', 'CC_NOTICE') AND address IS NOT NULL AND mail_id IS NOT NULL
            AND notice IS NULL)
        OR (kind = 'NOTICE' AND run_id IS NOT NULL AND mail_id IS NULL
            AND notice IS NOT NULL AND jsonb_typeof(notice) = 'object')
    )
);

CREATE UNIQUE INDEX inbox_items_seq
    ON inbox_items (seq);

CREATE INDEX inbox_items_unread_address
    ON inbox_items (address, seq)
    WHERE read_at IS NULL AND address IS NOT NULL;

CREATE INDEX inbox_items_unread_run
    ON inbox_items (run_id, seq)
    WHERE read_at IS NULL AND run_id IS NOT NULL;

CREATE TABLE kept_payloads (
    job_id uuid PRIMARY KEY,
    uploader jsonb NOT NULL CHECK (jsonb_typeof(uploader) = 'object'),
    message text NOT NULL,
    attachments jsonb NOT NULL CHECK (jsonb_typeof(attachments) = 'array'),
    uploaded_at timestamptz NOT NULL
);

CREATE TABLE job_results (
    result_id uuid_v7 PRIMARY KEY,
    job_id uuid_v7 NOT NULL REFERENCES jobs(id),
    produced_by_run_id uuid_v7 NOT NULL REFERENCES runs(id),
    addressed_run_id uuid_v7 NOT NULL REFERENCES runs(id),
    message text NOT NULL,
    attachments jsonb NOT NULL CHECK (jsonb_typeof(attachments) = 'array'),
    kept_at timestamptz NOT NULL,
    read_at timestamptz
);

CREATE INDEX job_results_addressed
    ON job_results (addressed_run_id);

CREATE TABLE saved_states (
    run_id uuid_v7 PRIMARY KEY REFERENCES runs(id),
    state jsonb NOT NULL,
    saved_at timestamptz NOT NULL
);

CREATE TABLE saved_state_agents (
    run_id uuid_v7 NOT NULL REFERENCES saved_states(run_id) ON DELETE CASCADE,
    agent_id uuid NOT NULL,
    has_inbox boolean NOT NULL,
    PRIMARY KEY (run_id, agent_id)
);

CREATE INDEX saved_state_agents_agent
    ON saved_state_agents (agent_id);

CREATE TABLE restart_origins (
    job_id uuid_v7 PRIMARY KEY REFERENCES jobs(id),
    restarts_run_id uuid_v7 NOT NULL REFERENCES runs(id)
);
