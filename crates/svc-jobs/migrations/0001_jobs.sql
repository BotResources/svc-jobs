-- Event-sourcing prerequisites for jobs.
--
-- Stamped because the major is registered EDA-FULL: the events are the source of
-- truth and every projection is rebuilt from them, so this store is the
-- foundation of the schema rather than a journal kept beside it. A major
-- registered SOFT or CRUD gets none of it — the posture decides this file, not
-- the manner, because the manner decides which crates exist and this is a
-- question about where truth lives.
--
-- Roles are deliberately NOT created here. `jobs_owner` (the migration role
-- this file runs as) and `jobs_app` (the least-privilege runtime role) are
-- declared by GitOps on the CNPG cluster, and a `CREATE ROLE` in a migration
-- would be a second, competing declaration of the same thing. The composition
-- root ensures the runtime role's password and grants at boot instead — see the
-- two-role comment in `src/lib.rs`.

-- The event store: append-only, and the source of truth for every aggregate in
-- this context. Nothing ever updates or deletes a row here; projections are what
-- change, and they are rebuilt from this table.
CREATE TABLE domain_events (
    id              UUID PRIMARY KEY,
    event_type      TEXT NOT NULL,
    aggregate_type  TEXT NOT NULL,
    aggregate_id    UUID NOT NULL,
    payload         JSONB NOT NULL,
    metadata        JSONB NOT NULL,
    occurred_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Hydration reads one aggregate's stream in order, so the aggregate index carries
-- `occurred_at` as its trailing column: the load is an index scan, never a sort.
CREATE INDEX idx_domain_events_aggregate
    ON domain_events (aggregate_type, aggregate_id, occurred_at);
CREATE INDEX idx_domain_events_type
    ON domain_events (event_type, occurred_at);
CREATE INDEX idx_domain_events_occurred_at
    ON domain_events (occurred_at);

-- Where each projector got to. Projections are rebuilt in the same transaction as
-- the append, so this is a crash-recovery and replay coordinate, not a lag meter.
CREATE TABLE projector_checkpoints (
    projector_name  TEXT PRIMARY KEY,
    last_event_id   UUID NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Projections that failed, kept whole so they can be replayed after the bug is
-- fixed. The payload is copied rather than referenced: the point is to replay the
-- exact event that broke, without depending on a join surviving the fix.
CREATE TABLE projector_dead_letters (
    id              UUID PRIMARY KEY,
    projector_name  TEXT NOT NULL,
    event_id        UUID NOT NULL,
    event_type      TEXT NOT NULL,
    event_payload   JSONB NOT NULL,
    error_message   TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_projector_dead_letters_projector
    ON projector_dead_letters (projector_name);
