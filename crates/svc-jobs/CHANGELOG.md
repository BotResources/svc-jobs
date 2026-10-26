# Changelog — svc-jobs

All notable changes to this service are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
the service adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

The version in [`Cargo.toml`](Cargo.toml) is the source of truth. CI gates every
PR on `scripts/check-changelog.sh`: whatever version sits in `Cargo.toml` must
have a matching plain `## x.y.z` heading here (the `0.0.0-dev` placeholder maps
to `## Unreleased`).

## 0.4.0 - 2026-10-26

Priorities and claim-based dispatch. Every 0.3 behaviour of a `PUSH` runner
type is kept; what changes for it is the order its waiting jobs are published
in.

### Added

- **Priorities.** A job requests a level from 1 (lowest) to 5 (highest), 3 by
  default, and waits at an effective level capped by the maximum of the user
  it runs for — or by `JOBS_DEFAULT_MAX_PRIORITY` (default 3) when that user
  has none or the job runs for nobody. A child job requests and waits at its
  parent's effective level. A manual-retry successor copies its predecessor's
  request, capped again (a forced level is not copied).
- Super-user mutations `jobsSetUserMaxPriority`, `jobsClearUserMaxPriority`
  and `jobsForceJobPriority`. Setting a maximum accepts any user id, observed
  by Jobs or not. A change of a user's maximum recalculates their waiting,
  non-forced root jobs and the waiting, non-forced descendants inheriting from
  them; forcing a job also forces its waiting, non-forced descendants. A
  descendant reached by either cascade records the inherited level as both its
  request and its effective level. A forced level holds until forced again.
- The user surface: `jobsMyMaxPriority` and `jobsMyMaxPriorityChanged`
  (snapshot, then every change), open to any active human and to nothing else.
- Job views carry `requestedPriority`, `effectivePriority` and
  `forcedPriority`; job affordances carry `force_priority`; job streams carry
  `JobsJobPriorityForcedEvent` and `JobsJobPriorityRecalculatedEvent`.
- **Dispatch order.** Highest effective priority first; within a level, strict
  round-robin between root jobs (least recently served root first, then the
  root whose oldest waiting job is oldest); then creation time. Both modes.
- **`CLAIM` dispatch mode** per runner type: instances claim their next run on
  `jobs.claim.{runner_type}` (core NATS request/reply, queue group
  `svc_jobs_claims`, served by every pod). A claim is held open up to
  `JOBS_CLAIM_MAX_WAIT_SECONDS` (default 30); the job is locked with
  `FOR UPDATE SKIP LOCKED` and its run created reserved to the claiming
  instance before the reply leaves. A claim repeated under the same `claim_id`
  is answered with the run already reserved. A reserved run not started within
  `JOBS_CLAIM_RESERVATION_SECONDS` (default 60) fails as transient with
  `claim_lost` and gets a cancel entry. A waiting job of a `CLAIM` type is
  cancelled in the ledger only; a reserved run is told to stop through the
  cancel bucket.
- **Per-runner-type settings**: `jobsSetRunnerTypeDispatchMode` and
  `jobsSetRunnerTypeMaxRunDuration` (platform administrators), the
  `dispatchMode` and `maxRunDurationSeconds` fields, the `set_dispatch_mode`
  and `set_max_run_duration` affordances, and the
  `RUNNER_TYPE_DISPATCH_MODE_CHANGED` / `RUNNER_TYPE_MAX_RUN_DURATION_CHANGED`
  fleet event kinds. A change applies to runs dispatched afterwards: a run
  carries the maximum duration it was dispatched with.
- `integration.cmd.jobs.job.create.v2` on its own durable
  (`svc_jobs_job_create_v2`): v1 plus an optional `priority`; out of range is
  rejected with `invalid_priority`. v1 is unchanged and requests 3.
- Migration `0003_priority_and_claim.sql` — schema only, no data rewritten:
  existing jobs read as requesting and waiting at 3, existing runner types as
  `PUSH` with a 72 h maximum, existing runs answer to the service default.

### Changed

- `JOBS_RUN_MAX_DURATION_SECONDS` is now the maximum a runner type is
  registered with, not a single service-wide maximum. Keep the `JOBS_CANCEL`
  bucket TTL above the largest maximum any type is set to.
- The run-duration backstop holds each run to the maximum it was dispatched
  with.
- `JobsManualRetryStartedEvent.runId` is nullable: the successor of a `CLAIM`
  type has no run until it is claimed. `JobsRun.instance` names the reserving
  instance of a claimed run before it starts; `JobsRun.reservedAt` and
  `JobsRun.maxRunDurationSeconds` are new.
- The GraphQL door admits an active human who is not a platform administrator
  for the user surface alone; every other request of theirs is still refused
  `FORBIDDEN` before a resolver runs.
- `job.queued.v1` is unchanged.
- No preemption: a started run is never interrupted for a higher priority.

### Known limitations

- Switching a runner type from `PUSH` to `CLAIM` does not withdraw triggers
  already published; they are consumed by the remaining push instances or end
  through the usual backstops.
- `jobs.trigger.{runner_type}` carries the deprecation intent of the contract;
  no deletion version is set yet.
- `contract-jobs` 0.6.0, `bc-jobs` 0.6.0.

## 0.3.4 - 2026-10-17

Dependency re-pin maintenance release. No wire, schema, migration or
configuration change for consumers.

- Every `br-rust-common` dependency moves from the legacy workspace tag
  `v1.3.0` to the per-crate tags `{crate}/v{version}` (`br-util-postgres`
  `0.1.1`, the others `0.1.0`). `br-test-harness` moves to
  `br-test-harness/v0.1.1`. The workspace resolves one copy of each `br-*`
  crate, matching `br-service-engine` 0.4.2.
- `br-util-postgres` 0.1.1 validates the database URL from the authority only
  and refuses a URL with no scheme, a foreign scheme or no explicit host.
  `ensure_app_role` refuses an empty password (a `JOBS_APP_PASSWORD` set but
  empty now fails the boot instead of clearing the `jobs_app` password), stores
  a SCRAM-SHA-256 verifier, and role creation and grants run under advisory
  locks. A deployed `DATABASE_URL` / `DATABASE_URL_OWNER` must name its host
  explicitly.
- `contract-jobs` and `bc-jobs` move to 0.5.1.
- Lockfile: `rustls` 0.23.46 and `quinn-proto` 0.11.19 (security advisories).

## 0.3.3 - 2026-09-28

Security release, lockfile bump only. No source, wire, schema, migration or
configuration change for consumers.

### Security

- `cargo update -p rustls-webpki -p rustls`: rustls 0.23.43 → 0.23.45,
  rustls-webpki 0.103.14 → 0.103.15 (RUSTSEC-2026-0285, CVSS 5.3: TLS 1.3
  handshake messages accepted across encryption-level boundaries). Every
  rustls path in svc-jobs is a TLS client: `sqlx-core (tls-rustls)` to
  Postgres and `async-nats → rustls` to NATS.
- chacha20 0.10.2, spin 0.9.9 and event-listener 5.4.2 are already clean. The
  only advisory left in `cargo audit` is RUSTSEC-2023-0071 (rsa): it is listed
  in `Cargo.lock` only and not compiled. Its ignore in `deny.toml` now records
  the upstream status with the date it was checked (2026-09-28: no fixed rsa
  version), as the file's own policy requires.
- The lockstep publish republishes chart `br-svc-jobs` at 0.3.3 with unchanged
  templates and values. `charts/br-svc-jobs/Chart.yaml` is not edited:
  `scripts/publish.sh` stamps `version` and `appVersion` from the crate version
  when it packages the chart.

## 0.3.2 - 2026-09-08

Dependency-bump maintenance release. No wire, schema or behaviour change for
consumers.

- Rebuilt on `br-rust-common` v1.3.0 (from v1.2.0) and the `br-e2e-harness`
  test harness v1.2.0 (from v1.1.3). Both are additive minor releases; the
  service source is unchanged and every gate is green on the new pins.
- `bc-jobs` and `contract-jobs` move to 0.5.0 (minor, not patch): their public
  API surfaces `br-rust-common` types — the `bc-jobs` ports and domain
  references expose `br_core_events` types, and the `contract-jobs`
  `integration` feature returns `br_core_integration` coordinate types — so a
  rebuild on a new `br-rust-common` is a contract-relevant change for their
  consumers. The `contract-jobs` default (runner) footprint pulls no
  BotResources library and stays byte-identical.
- Helm chart realigned to 0.3.2 (`version` and `appVersion`) with the crate.

## 0.3.1 - 2026-09-01

- Terminal run facts are read through the closed failure-kind vocabulary of
  contract-jobs 0.4.0. A kind outside `TRANSIENT`/`PERMANENT` no longer reaches
  the domain at all; the settling path is unchanged, so such a fact is still
  acknowledged and discarded rather than redelivered forever.
- The status consumer dispatches on `RunnerStatusFact` from contract-jobs
  instead of its own string literals, and reads a status subject exactly once:
  the identity it resolved is what settles the frame and what names the metric,
  so the two can never disagree about which fact was dropped. One deliberate
  side effect: a frame naming no known fact is now acknowledged before its
  runner type is validated, so a subject invalid on both counts settles as an
  ack instead of a term — both settle the message definitively, nothing is
  redelivered either way.
- A runner fact this service could not read, or refused before it reached its
  job, now raises `jobs_runner_facts_discarded_total{fact}` on `/metrics`,
  alongside the warning it already logged. The two terminal series are
  registered at zero when the transport starts, so the alert is armed before the
  first drop. A fact the domain examined and declined is not counted — it was
  understood, not lost.

## 0.3.0 - 2026-08-27

- Added independent durable consumers for `job.cancel.v2`, `job.finish.v2`,
  and `job.fail.v2`; the version 1 durable names are deliberately unchanged so
  their JetStream cursors survive the upgrade.
- Version 2 commands enter the lifecycle-only domain path and retain the
  envelope actor as declarant attribution. Version 1 continues through the
  legacy owner-coherence path.
- Added a real Postgres/NATS oracle covering both versions, stable-command-id
  redelivery, lifecycle refusal after terminal resolution and soft deletion,
  exact failure cause and resolution identities, subscriptions, query state,
  affordances, and actor attribution.
- The v2 cancellation oracle exercises the complete tree semantics: descendant
  cascade, in-flight desired-state cancel entries, undelivered-trigger
  withdrawal, late runner completion, and one durable resolution per job.

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
