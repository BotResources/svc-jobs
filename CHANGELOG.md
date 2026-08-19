# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Release headings are plain `## x.y.z` — the release pipeline greps that exact
form to decide whether a version ships.

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
