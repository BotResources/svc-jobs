# Changelog — svc-jobs

All notable changes to this service are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
the service adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

The version in [`Cargo.toml`](Cargo.toml) is the source of truth. CI gates every
PR on `scripts/check-changelog.sh`: whatever version sits in `Cargo.toml` must
have a matching plain `## x.y.z` heading here (the `0.0.0-dev` placeholder maps
to `## Unreleased`).

## 0.1.0 - 2026-08-17

- The service: migrations realising the sealed 0.1 database schema (including
  the bounded `run_logs` range partitions and the three read views), the
  PostgreSQL adapters behind the `bc-jobs` ports, the GraphQL edge serving the
  sealed SDL (five queries, three ack-only mutations, four snapshot-then-delta
  subscriptions over SSE), the integration bus through
  `br-util-nats-fabric` (four durable command consumers, transactional outbox
  for the eight published events), the confined runner transport (trigger
  publication and withdrawal, status and log consumption, the desired-state
  cancel bucket, the presence watch), and the dispatch and backstop loops.
- Every knob is configuration, validated once at boot. The declared
  environment: `PORT`, `DATABASE_URL`, `DATABASE_URL_OWNER`, `NATS_URL`,
  `NATS_USER`, `NATS_PASSWORD`, `JOBS_APP_PASSWORD`; the domain and timing
  knobs `JOBS_INACTIVITY_TIMEOUT_SECONDS`, `JOBS_RUN_MAX_DURATION_SECONDS`,
  `JOBS_RETRY_BASE_DELAY_SECONDS`, `JOBS_RETRY_MAX_DELAY_SECONDS`,
  `JOBS_RETRY_FACTOR`, `JOBS_RETRY_JITTER_BASIS_POINTS`,
  `JOBS_MAX_ATTEMPTS_CEILING`, `JOBS_BACKSTOP_INTERVAL_SECONDS`,
  `JOBS_DISPATCH_MINIMUM_WAKE_MILLISECONDS`; the runner-transport consumer
  knobs `JOBS_CONSUMER_ACK_WAIT_SECONDS`, `JOBS_CONSUMER_MAX_ACK_PENDING`,
  `JOBS_CONSUMER_MAX_DELIVER`; and the background-task supervision knobs
  `JOBS_TASK_RESTART_INITIAL_BACKOFF_MILLISECONDS`,
  `JOBS_TASK_RESTART_MAX_BACKOFF_SECONDS`, `JOBS_TASK_RESTART_BUDGET`,
  `JOBS_TASK_STABILITY_SECONDS`; and the capacity knob
  `JOBS_LOG_PARTITION_HORIZON_WARNING_DAYS`.
- The runner-transport durable consumers declare their own `ack_wait`,
  `max_ack_pending` and `max_deliver` instead of inheriting the server
  defaults, so a handler slower than the server's grace period is never
  redelivered mid-processing.
- Concurrent writers are refused on the whole lifecycle state their decision
  read, not on a coarse summary of it: a job write re-reads the locked
  aggregate and compares every run's start, terminal, plan, steps, cancellation
  request and retry schedule, plus the resolution, the deletion and the manual
  retry; a fleet write locks its runner type and compares every live instance's
  session, version, reported status and change number. A duplicate or
  redelivered fact handled by two instances at once therefore writes one domain
  event and publishes one integration event, never two.
- Every background task is supervised: the death of any of them (error, silent
  return or panic) takes readiness DOWN with the task named as the reason and
  restarts it with bounded exponential backoff; readiness comes back UP only
  once every task runs again, and a task that exhausts its restart budget
  leaves the pod NOT READY for an operator. When the durable fact listener
  dies, every open subscription is terminated so clients reconnect onto a fresh
  snapshot on a healthy pod rather than freezing on a stream that stopped.
- A `CHECK` violation from PostgreSQL is treated as an infrastructure failure —
  redelivered and alerted — never as a domain refusal that discards a lifecycle
  fact.
- Settling a job writes the affordance changes it causes on its predecessor and
  on its descendants in the SAME transaction as the settling fact, on rows
  locked in one sorted pass with it. A crash can no longer land a terminal fact
  while losing the affordance refresh that fact caused, and no client is left
  with a stale action until it reconnects.
- Boot reports the `run_logs` partition horizon read from the live schema: the
  service says loudly how long log ingestion is covered, and errors once the
  remaining margin falls under `JOBS_LOG_PARTITION_HORIZON_WARNING_DAYS` or the
  horizon is exhausted. It never refuses to boot on it — the sealed spec ranks
  a lost log line below a lost lifecycle fact, so log capacity never takes the
  lifecycle path down.
- `jobsLogs` refuses `first` and `last` together with `first_and_last_together`
  instead of silently mixing one argument's direction with the other's limit.
- A subscriber that falls irrecoverably behind the in-process fact stream ends
  its subscription instead of skipping the facts it missed: the client
  reconnects onto a fresh snapshot, and no list silently keeps a stale row.
- Watching a job watches its whole tree: every fact carried by a descendant, and
  every descendant joining the tree, pushes the delta that carries the refreshed
  projection.
- The whole user-facing surface is platform-administrator only: a missing or
  undecodable passport is answered `UNAUTHENTICATED`, any other caller
  `FORBIDDEN`, both as structured GraphQL errors, before a query, a mutation or
  a subscription establishment reaches a resolver.
- `run_logs` is range-partitioned by month with no `DEFAULT` partition, and the
  migration declares partitions up to `2030-12-31`. This is deliberate — a
  missing partition fails loud rather than silently pooling rows — and it is an
  operational obligation: partitions covering the next months must be declared
  before the horizon is reached, or log ingestion stops and the log consumer
  redelivers indefinitely.
- Declared NATS streams and buckets are bound, never created: an absent stream
  or bucket fails the boot loudly — readiness goes down, the failure is logged
  and the process stops with a non-zero code rather than lingering as a pod that
  will never be ready.
