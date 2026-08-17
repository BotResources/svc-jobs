# Changelog — bc-jobs

All notable changes to this bounded context are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
the crate adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The version in [`Cargo.toml`](Cargo.toml) is the source of truth; release
headings are plain `## x.y.z` (the `0.0.0-dev` placeholder maps to
`## Unreleased`).

## 0.1.0 - 2026-08-17

- The Job and RunnerType aggregates, their commands, events, affordances and
  policies, with the double-barrier invariants.
- A runner instance's reported status is the closed `ReportedStatus`
  (`READY` | `DRAINING`); a runner type is available only while at least one of
  its live instances is `READY`, so an all-draining type blocks dispatch while
  the runs its instances already carry keep going untouched.
- An observed presence loss names the session it was observed on: closing a
  session the instance no longer holds is refused (`stale_loss`), so a loss
  that finds the process reconnected under a new session cannot close it or
  orphan the runs it is carrying.
- A runner instance declares a `Capacity` (at least 1, at most 10 000, so the
  stored number is exact by construction) alongside its status. An instance is
  busy once its current runs reach that capacity, and a runner type
  reports the total capacity of its live, non-draining instances. A capacity
  change is recorded as the same presence fact as a status change: presence is
  one self-declared record the runner rewrites whole, so the fleet keeps one
  fact per rewrite instead of inventing per-field facts the source never
  distinguishes.
- `ServiceLimits` and `RetryPolicy` are constructed through a validating `new`
  and expose their figures through accessors: a default budget above its own
  ceiling, a non-positive duration, a zero retry factor, a maximum delay under
  the base delay and an out-of-range jitter span are refused at construction, so
  a misconfigured deployment dies at boot rather than job by job.
- `JobEvent::decode` / `FleetEvent::decode` read a stored event back into its
  typed fact, so a subscriber on any pod renders the same fact the writer wrote.
- An identical plan re-declaration is absorbed instead of opening a second
  declaration.
- A job is waiting for its runner type only while its next attempt is still
  undispatched; a dispatched trigger belongs to the type, and `RunDispatched`
  is what stops a job waiting.
- `RunTrigger` carries the attempt number and the user the work runs for, and
  `RunnerTransport` can withdraw an undelivered trigger — both required by the
  published runner-transport contract.
