# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Release headings are plain `## x.y.z` — the release pipeline greps that exact
form to decide whether a version ships.

## Unreleased

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
  `v1.3.0` to the per-crate tags `{crate}/v{version}`: `br-core-auth`,
  `br-core-events`, `br-core-integration`, `br-util-axum-readiness`,
  `br-util-graphql`, `br-util-observability` and `br-util-nats-fabric` at
  `0.1.0`, `br-util-postgres` at `0.1.1`. `br-test-harness` moves from
  `v1.2.0` to `br-test-harness/v0.1.1` (same features: `e2e-db`, `server`,
  `spawned-nats`). The workspace resolves one copy of each `br-*` crate.
- Why: a service built on `br-service-engine` 0.4.2 (svc-threads) names the
  `br-rust-common` types at per-crate tags. `contract-jobs` 0.5.0 pinned
  `br-core-integration` at `v1.3.0`, so such a service resolved two copies of
  `CommandCoords` / `EventCoords`. `contract-jobs` 0.5.1 carries the per-crate
  pins; consume it at the git tag `contract-jobs/v0.5.1`.
- `bc-jobs` and `contract-jobs` move to 0.5.1 (patch): public API and wire are
  unchanged.
- `br-util-postgres` 0.1.1 validates the database URL from the authority only
  and refuses a URL with no scheme, a foreign scheme or no explicit host. Its
  `ensure_app_role` refuses an empty password (it used to clear the role
  password silently), stores a SCRAM-SHA-256 verifier computed in the process
  instead of sending the clear password, and both `ensure_app_role` and
  `grant_app_access` run under advisory locks so replicas starting together no
  longer race. The DSNs the e2e fixtures build name an explicit host, so
  nothing is refused there. A deployed `DATABASE_URL` / `DATABASE_URL_OWNER`
  must name its host explicitly, and a `JOBS_APP_PASSWORD` that is set but
  empty now fails the boot instead of clearing the `jobs_app` password.
- Lockfile: `rustls` 0.23.45 -> 0.23.46 (RUSTSEC-2026-0346 to -0349) and
  `quinn-proto` 0.11.16 -> 0.11.19 (RUSTSEC-2026-0338 to -0342, listed in
  `Cargo.lock` only and not compiled). New transitive crates come from
  `br-util-postgres` 0.1.1 (`postgres-protocol` and its digest stack).
- The lockstep publish republishes chart `br-svc-jobs` at 0.3.4;
  `charts/br-svc-jobs/Chart.yaml` is not edited, `scripts/publish.sh` stamps it.

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

### Changed

- The failure kind a runner declares on a terminal fact is now a closed
  vocabulary in the published contract (`contract-jobs` 0.4.0): `TRANSIENT` or
  `PERMANENT`, absent still meaning permanent. The offer always said so; the
  type said `Option<String>`, so a runner could publish any word, compile, ship,
  and have its terminal fact discarded by the receiver — a job left in progress
  with nothing to end it, as happened in dev on 2026-08-25 with
  `"kind": "scaffold"`. Wire-identical for a conforming runner; a
  non-conforming one now fails to compile.

### Added

- `jobs_runner_facts_discarded_total{fact}` on `/metrics`: the count of runner
  facts this service could not read or accept, acknowledged and dropped. A
  discarded terminal fact leaves a run open until a backstop reclaims it, and a
  warning line is not something an operator can alert on.
- A regression scenario against real infrastructure: a terminal fact naming a
  kind outside the vocabulary leaves the job untouched, raises the counter, and
  does not park the status stream — the next conforming fact still lands.

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
