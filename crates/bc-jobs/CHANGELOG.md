# Changelog — bc-jobs

All notable changes to this bounded context are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
the crate adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The version in [`Cargo.toml`](Cargo.toml) is the source of truth; the
`0.0.0-dev` placeholder this crate ships with accumulates its changes under
`## [Unreleased]` until the first release promotes them to a numbered heading.

## [Unreleased]

- Scaffolded by `runkit`: the doctrinal module tree, generated whole and empty.
  No command, no event, no port yet.
- The Job and RunnerType aggregates, their commands, events, affordances and
  policies, with the double-barrier invariants.
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
