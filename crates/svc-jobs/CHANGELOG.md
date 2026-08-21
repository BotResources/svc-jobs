# Changelog — svc-jobs

All notable changes to this service are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
the service adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

The version in [`Cargo.toml`](Cargo.toml) is the source of truth. CI gates every
PR on `scripts/check-changelog.sh`: whatever version sits in `Cargo.toml` must
have a matching plain `## x.y.z` heading here (the `0.0.0-dev` placeholder maps
to `## Unreleased`).

## 0.2.0 - 2026-08-21

- Added the runner-type lifecycle migration and canonical Postgres adapter,
  including set-based decision-fact reads for snapshots and a locked retirement
  recheck that prevents an active-job race.
- Added ack-only administrator mutations for deprecation, reactivation, and
  retirement. Fleet reads and every typed fleet delta carry lifecycle and the
  complete current affordance set from the shared domain decisions.
- Added the Published Language adapter for `jobs.runner_type.*`; active and
  deprecated types are put, retired types retracted, and startup reconciles the
  full prefix. A post-commit KV failure is logged and healed later rather than
  turning a committed mutation into a false error verdict.
- Separated Job routing keys from presence-registered RunnerType aggregates;
  added per-pod periodic catalog reconciliation with readiness health —
  convergence is by reconciliation, with no cross-pod write ordering — and a
  durable, idempotent quiet-period affordance impact.

## 0.1.1 - 2026-08-19

- **The service survives a restart.** Boot no longer runs `ensure_app_role`
  unconditionally: a probe (`role_password_already_works`, the same guard
  svc-identity, svc-projects, svc-services, svc-tasks, svc-timesheet and
  svc-website carry) first asks whether `jobs_app` already accepts the
  configured password, and provisioning is skipped when it does. Under
  PostgreSQL 16/CNPG the unconditional `ALTER ROLE jobs_app PASSWORD …` was
  denied on every boot after the first (`permission denied to alter role`),
  crashlooping the pod. Non-credentials probe failures fail the boot loudly
  instead of falling through to the denied ALTER. Fixes contract breach
  `01a019f6-cdd1-783f-9aff-6700fcc02ed5` (registry patch 0.1.1).

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
- A creation names its producing bounded context or it is refused: `producer` is
  required on `job.create`, so a payload omitting it does not deserialize and
  takes the malformed path — answered by a `creation_rejected` event carrying
  `malformed_payload` whenever the payload still names a job id. There is no
  fallback attribution: no source-derived key, no parent-derived key, no
  reserved key. A job is never recorded under a producer nobody declared, and a
  `source_bc` that disagrees with `producer` is refused.
- A presence entry announces `READY` or `DRAINING`. A `DRAINING` instance stays
  live and keeps the runs it already carries, but is excluded from its runner
  type's availability: while every live instance of a type drains, the type
  reports unavailable and dispatch waits instead of publishing a trigger nobody
  would take. An unknown status code does not parse, so the entry takes the
  unreadable path: it is logged, never read as `READY`, and it drains the live
  instance that wrote it.
- A presence entry also declares a `capacity` (required, at least 1): how many
  runs the instance carries at once. It is declarative — dispatch stays
  pull-based and capacity never gates delivery — and feeds the fleet reads: an
  instance is busy once its runs reach its declared capacity, and a runner type
  totals the capacity of its live, non-draining instances. A capacity that is
  absent, zero or negative never parses on the wire; one above the domain
  ceiling of 10 000 parses but is refused by the domain. Either way the entry
  is logged and refused, and the refusal drains the live instance that wrote it
  — the instance keeps its session and its runs, takes no new work, and the
  fleet keeps the last capacity it accepted.
- A presence entry an instance rewrote in an unreadable form is treated as
  `DRAINING` when that instance holds a live session: it keeps its session and
  its runs but takes no new work, instead of staying dispatchable for ever on
  the last readable report it managed to write. An unreadable entry naming no
  live instance is still ignored and logged.
- The backstop reconciles the presence bucket with the open sessions: it reads
  the bucket's keys once per sweep and closes, under `presence_expired`, every
  open session the bucket no longer backs, reclaiming its runs. It covers the
  two evictions no watch can see — one that lands while the pod is down, and
  one whose recording was abandoned under contention. The reconciliation is
  best-effort: a broker the sweep cannot reach is logged, and the backstops
  that need PostgreSQL alone still run.
- A reclaim takes the runs of the session that was lost, not every run the
  instance key ever carried: only a run started inside the closed session's
  window is failed for instance loss, so a run the replacement session is
  executing survives its predecessor's reclaim.
- The dispatch loop no longer spins when a runner type is fully draining: a
  pass that skipped a job because nobody would take it waits its full interval
  and is woken by the fleet fact of an instance reporting ready again, rather
  than re-reading a retry that is due but undispatchable every few hundred
  milliseconds. That long wait is taken only when nothing else in the pass asks
  for a sooner one — a dispatch that failed, or one that filled its batch,
  keeps the short wake, so a due retry on an available type is never delayed by
  an unrelated skip.
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
