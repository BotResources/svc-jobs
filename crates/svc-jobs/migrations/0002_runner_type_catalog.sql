CREATE DOMAIN runner_type_lifecycle AS text
    CHECK (VALUE IN ('ACTIVE', 'DEPRECATED', 'RETIRED'));

CREATE TABLE registered_runner_types (
    runner_type_id uuid_v7 PRIMARY KEY REFERENCES runner_types(id),
    lifecycle runner_type_lifecycle NOT NULL,
    registered_at timestamptz NOT NULL
);

INSERT INTO registered_runner_types (runner_type_id, lifecycle, registered_at)
SELECT rt.id, 'ACTIVE', min(ps.connected_at)
FROM runner_types rt
JOIN runner_instances ri ON ri.runner_type_id = rt.id
JOIN runner_presence_sessions ps ON ps.instance_id = ri.id
GROUP BY rt.id;

CREATE TABLE runner_type_affordance_impacts (
    runner_type_id uuid_v7 NOT NULL REFERENCES registered_runner_types(runner_type_id),
    terminal_run_at timestamptz NOT NULL,
    PRIMARY KEY (runner_type_id, terminal_run_at)
);

CREATE INDEX runner_type_affordance_impacts_due
    ON runner_type_affordance_impacts (terminal_run_at, runner_type_id);
