# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Release headings are plain `## x.y.z` — the release pipeline greps that exact
form to decide whether a version ships.

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

- **Owner authorization is enforced as "declaring actor", not as the spec's
  "Owner".** A resolution command (`job.finish`, `job.fail`, `job.cancel`) is
  accepted when its envelope actor is the actor that declared the job (walking
  back a manual-retry chain to its origin). Jobs has no actor-to-producer-key or
  actor-to-runner-identity directory in 0.1, so the caller identity cannot be
  resolved into the domain's `Caller` independently of the aggregate. The
  structural owner guard still runs, but the effective rule is the declaring
  actor. Consequence: a bounded context that declares a child under a parent
  owned by another runner becomes that child's declarer and can resolve it,
  while the parent's runner cannot. Operator arbitration is required before
  0.2.
- **A run trigger lost after commit is not reclaimed.** The trigger is published
  to the runner transport after the transaction commits; a publish failure is
  logged and the job stays `IN_PROGRESS` with a dispatched, never-started run.
  Neither the run-duration backstop (which requires a start) nor the inactivity
  backstop (which requires no active run) picks it up. This is the non-crash
  twin of the arbitrated pending-run gap; recovery is cancel or manual retry.
- **`run_logs` partitions run from 2026-01 to 2030-12.** A runner clock beyond
  that window makes the insert fail; the line is refused permanently
  (terminated, never redelivered forever), but it is lost.
