# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

- svc-jobs 0.1.0: the complete service — `contract-jobs` (published language),
  `bc-jobs` (pure domain) and `svc-jobs` (edge, adapters, bus, runner transport,
  dispatch and backstop loops) — verified end to end against real PostgreSQL and
  real NATS by the twelve registry scenarios.

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
  that window makes the insert fail; the line is now refused permanently
  (terminated, never redelivered forever), but it is lost.

- Repository bootstrap: governance files (LICENSE, CONTRIBUTING, SECURITY, SUPPORT) ahead of the service scaffold.
- CI/CD workflows mirroring the sibling services, pre-scaffold-safe: a `scaffold probe` job skips the Rust checks until `Cargo.toml` lands; CD's detect-bump no-ops without a Cargo workspace. Branch protection script (`scripts/setup-branch-protection.sh`) as the declarative required-checks source of truth.
