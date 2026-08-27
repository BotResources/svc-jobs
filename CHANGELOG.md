# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Release headings are plain `## x.y.z` — the release pipeline greps that exact
form to decide whether a version ships.

## Unreleased

## 0.3.0 - 2026-08-27

### Added

- Version 2 of the `job.cancel`, `job.finish`, and `job.fail` integration
  commands, each on its own durable consumer while the version 1 cursors and
  behavior remain untouched.
- Real-infrastructure coverage proving the version boundary: a non-owner is
  refused by version 1 and may declare the same lifecycle transition through
  version 2, with actor metadata preserved as attribution on the emitted fact.
- Release pipeline integration with the production Services registry:
  `scripts/registry-gate.sh` (sealed-and-not-implemented gate at PR time,
  pre-build, and pre-push), `scripts/registry-docs.sh` (procedural SDL + DB
  schema posed from the built artifacts before the image push), and
  `scripts/registry-implement.sh` (image record + advisory implemented-flip
  probe after the push). CI/CD only — no service behavior changes.

### Changed

- Version 2 resolution commands authorize no producer ownership inside Jobs.
  The integration fabric owns admission; Jobs validates only its lifecycle
  invariants and records the envelope actor as the declarant. Version 1 keeps
  its legacy envelope-actor/owner coherence guard for compatibility.

## 0.2.0 - 2026-08-21

### Added

- Runner types are durable lifecycle entities (`ACTIVE`, `DEPRECATED`,
  `RETIRED`) with administrator mutations, backend-owned action affordances,
  and complete snapshot-then-delta fleet updates.
- Active and deprecated runner types are projected to Published Language under
  `jobs.runner_type.{runner_type}`. Retirement retracts the entry; boot performs
  a full repair pass so a missed post-commit KV write is recoverable.
- PostgreSQL migration `0002_runner_type_catalog.sql` separates historical Job
  routing keys from registered aggregates and backfills only types proven by
  historical presence.
- A durable timer emits `RunnerTypeBecameRetirable` when the retirement quiet
  period elapses without another state transition. Its interval is
  `JOBS_RUNNER_TYPE_IMPACT_INTERVAL_SECONDS` (default 60 s). The decision is
  per runner type: a sweep that comes back after an outage consumes every
  overdue impact of a type together and emits the fact once.
- `JOBS_CATALOG_RECONCILE_INTERVAL_SECONDS` (default 30 s) configures the
  reconciliation that repairs the whole published runner-type prefix. The Helm
  chart surfaces it alongside the other runner-type knobs.
- `JOBS_RETIREMENT_QUIET_PERIOD_SECONDS` (default 86400) configures the quiet
  period a deprecated runner type must observe before it may retire. The
  Helm chart surfaces it, and the sweeper ages the stored terminal instant
  against the configured value at read time — nothing is materialized.

### Changed

- Retired types refuse new jobs and dispatch while still recording presence.
  Reactivation from retired requires a live instance; retirement requires a
  deprecated type, no non-terminal jobs, and the configured quiet period after
  the latest terminal run.
- **A runner type no instance has ever announced no longer appears in
  `jobsFleet` or `jobsFleetChanged`.** A key referenced only by a job is a
  routing entry, not a registered aggregate: it has no lifecycle, no fleet and
  no affordances to answer with. A job may still be created against it and
  waits, as before — there is simply no fleet row standing for it until the
  first instance announces itself.
- Reactivation refused on an already-active runner type answers
  `runner_type_already_active` instead of `runner_type_not_deprecated`, which
  was misleading: a retired type is reactivatable too.
- Published Language writes happen after their transaction commits — never with
  a transaction held open across the KV round-trip — and only for a fact that
  moves a lifecycle: a job commit, a heartbeat or a status flip cannot change a
  runner type's lifecycle and no longer touches the bucket. Two pods writing the
  same key are not ordered against each other; convergence is by reconciliation,
  which also repairs a write lost to a crash. A bucket outage removes the pod
  from readiness, and only a complete reconciliation puts it back — a single-key
  projection can lower readiness, never restore it.
- **Readiness now means every background task has bound its source of work.**
  A supervised task used to count as up from the moment it was spawned, so the
  pod could report READY while a task was still opening its subscription, and a
  restart restored readiness before the retry had re-established anything. Each
  task now starts out down and signals establishment itself — the presence watch
  after its KV watch exists, the durable fact listener after `LISTEN` is bound,
  the JetStream consumers after their durable is bound, the timer loops at
  entry. `GET /readyz` stays DOWN, naming the task, until all of them report in.

### Fixed

- **One unreadable stored runner type no longer takes the whole fleet down.**
  A presence session whose status row was unreadable failed `jobsFleet` and
  every fleet delta for every runner type. An unfiltered listing now leaves
  that one type out and logs the corruption with its code, while asking for
  that type by name — and every command path — still fails explicitly: the
  state that cannot be loaded is still refused, it just no longer takes its
  neighbours with it.
- **A runner announcing itself during a gap in the presence watch is no longer
  invisible until its next heartbeat.** The watch replayed nothing: a presence
  entry written while the watch was being established — at boot under load, or
  across a supervised restart — was missed for good, because reconciliation only
  closes sessions whose KV entry vanished, never the reverse. The watch now
  replays the last entry per key (deletes included) before live updates;
  processing was already idempotent, so a replayed entry for a known session
  produces no event. Combined with the readiness change above, a subscriber
  connected to a READY pod can no longer sit on a snapshot that never moves.
- **A dropped `LISTEN` connection no longer leaves subscribers on a live but
  silent stream.** The durable fact listener used `PgListener::recv`, which
  reconnects on its own and loses every `NOTIFY` raised while it was away —
  the connection came back, the subscriptions stayed open, and the deltas for
  that window were simply gone. The listener now surfaces the loss, so the
  supervisor restarts it, its teardown ends every subscription, and clients
  reconnect onto a fresh snapshot while readiness is DOWN in between.

## 0.1.1 - 2026-08-19

### Fixed

- **The service survives a restart.** `svc-jobs` provisioned its runtime role
  unconditionally at boot: `ensure_app_role` guards its CREATE with `IF NOT
  EXISTS` but then runs `ALTER ROLE jobs_app PASSWORD …` every time. Under
  PostgreSQL 16 (`createrole_self_grant=''`) that ALTER is denied on every boot
  after the first — the implicit membership `jobs_owner` gains by creating
  `jobs_app` is revoked by the CNPG roles reconciler, and CREATEROLE alone no
  longer confers authority over a role the grantee holds no ADMIN OPTION on.
  The first boot succeeded and every later one failed
  `permission denied to alter role`, exiting the pod into CrashLoopBackOff
  behind a stuck rollout. Boot now probes whether `jobs_app` already accepts the
  configured password and skips provisioning when it does; a probe failure that
  is not credentials-class (`28P01` / `28000` / `3D000`) fails the boot loudly
  rather than falling through to the denied ALTER. This is the guard the
  sibling services already carry, ported unchanged.

## 0.1.0 - 2026-08-17

First release — the complete service, verified end to end against real
PostgreSQL and real NATS JetStream by the twelve sealed registry scenarios.

### Added

- `contract-jobs` 0.1.0 — the published language: integration command/event
  subjects and typed payloads, the runner-transport wire (trigger, status, log,
  cancel-bucket and presence shapes, stream and bucket names), and the validated
  `SubjectSegment` grammar that keeps every rendered subject narrow.
- `bc-jobs` 0.1.0 — the pure domain: the Job and RunnerType aggregates, their
  commands, events, affordances and policies, with double-barrier invariants
  (every rule enforced at write time and re-validated at hydration).
- `svc-jobs` 0.1.0 — the service binary: PostgreSQL adapters and migrations
  realising the sealed 0.1 schema, the GraphQL admin edge serving the sealed
  SDL (platform-administrator only), the integration bus over
  `br-util-nats-fabric` (four durable command consumers, transactional outbox
  for the eight published events), the confined runner transport (trigger
  publication and withdrawal, status and log consumption, desired-state cancel
  bucket, presence watch), and the supervised dispatch and backstop loops.
- **Who may resolve a job**: `job.finish`, `job.fail` and `job.cancel` are
  accepted only when the command envelope declares the same actor that declared
  the job, inherited along a manual-retry chain so a successor answers to the
  actor that declared its origin. In practice the parent's runner finishes,
  fails or kills its child; the producing bounded context resolves the root it
  declared; platform administrators act through the restricted GraphQL surface.
  This is a coherence rule between self-declared identities, not an
  authentication: nothing on the bus verifies the actor a publisher writes into
  its envelope. Trust on the bus is rooted in NATS access — first-party
  publishers on a network-isolated segment — and impersonation there is out of
  scope by design.
- A presence entry announces `READY` or `DRAINING`. A `DRAINING` instance stays
  live and keeps the runs it already carries, but is excluded from its runner
  type's availability: while every live instance of a type drains, the type
  reports unavailable and dispatch waits instead of publishing a trigger nobody
  would take.
- A presence entry also declares a `capacity` (required, at least 1): how many
  runs the instance carries at once. It is declarative — dispatch stays
  pull-based and capacity never gates delivery — and feeds the fleet reads: an
  instance is busy once its runs reach its declared capacity, and a runner type
  totals the capacity of its live, non-draining instances.
- A presence entry this service refuses — unreadable, carrying an unknown status
  code, or declaring a capacity outside the range the domain accepts — is never
  read as `READY`, and never silently dropped. A live instance that wrote it is
  recorded as `DRAINING`: it keeps its session and the runs it carries, takes no
  new work, and comes back into service on the next entry the domain accepts. An
  entry naming no live instance is ignored. Either way the refusal is logged
  with its code.
- Release tooling: `scripts/publish.sh` (static-musl cargo-zigbuild build,
  multi-arch image `ghcr.io/botresources/br-svc-jobs` + Helm chart
  `charts/br-svc-jobs`, image-first tag-after), the Dockerfile, and the
  per-crate `cargo-semver-checks` gate on `contract-jobs` in CI.

### Known limitations of 0.1

- **A run trigger lost after commit is not reclaimed.** The trigger is published
  to the runner transport after the transaction commits; a publish failure is
  logged and the job stays `IN_PROGRESS` with a dispatched, never-started run.
  Neither the run-duration backstop (which requires a start) nor the inactivity
  backstop (which requires no active run) picks it up. Recovery is a
  cancellation or a manual retry.
- **A presence loss seen on the watch names no session on the wire.** The
  watching pod pins the session it reads from the fleet on its first attempt,
  so a pod delayed between that read and its write can still close a session
  that replaced the one it observed — a narrow multi-pod window. The damage is
  bounded to that session's own window (the reclaim only takes runs started
  inside it). Carrying a runner boot identity in the presence entry closes the
  gap and is a 0.2 contract change.
- **`run_logs` partitions run from 2026-01 to 2030-12.** A log line whose
  timestamp falls outside that window fails to insert, and the log consumer
  reads that failure as an infrastructure failure: the line is never terminated
  — it is negatively acknowledged and redelivered indefinitely (the delivery
  budget is unlimited) until partitions covering it are declared. Lifecycle
  facts are unaffected. Declare the next partitions before the horizon;
  `JOBS_LOG_PARTITION_HORIZON_WARNING_DAYS` makes the boot log the remaining
  margin as an `error!`.
- **A job addressed to a runner type that never appears waits indefinitely.** It
  stays `PENDING`, with no run, no deadline and no expiry: the inactivity
  backstop watches `IN_PROGRESS` jobs and nothing reclaims a pending one. The
  wait ends when an instance of that type finally connects, or when someone
  cancels the job. This is intended — jobs never invents a failure for work a
  fleet may still take — but a producer that wants a deadline must impose it
  itself.
