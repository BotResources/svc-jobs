CREATE TABLE run_triggers (
    run_id uuid_v7 PRIMARY KEY REFERENCES runs(id),
    stream_sequence bigint NOT NULL,
    CHECK (stream_sequence > 0)
);

CREATE INDEX run_triggers_stream_sequence
    ON run_triggers (stream_sequence);
