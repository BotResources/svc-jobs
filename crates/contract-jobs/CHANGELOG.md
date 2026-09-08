# Changelog — contract-jobs

All notable changes to the published language of svc-jobs are documented
here. This crate is the inter-BC contract: a breaking change here ripples to
every consumer, so each version is deliberate. Release headings are plain
`## x.y.z` — the release pipeline greps that exact form before tagging.

## 0.5.0 - 2026-09-08

- Rebuilt on `br-rust-common` v1.3.0 (from v1.2.0). No change to the published
  subjects, KV prefixes, payload shapes or the runner-transport wire — a
  conforming producer or consumer is unaffected.
- Minor rather than patch because the `integration` feature's public API
  returns `br_core_integration` coordinate types (`CommandCoords`,
  `EventCoords`, `CoordError`): the `br-rust-common` these resolve against is
  part of this crate's contract, so a platform consumer enabling `integration`
  must agree on `br-rust-common` v1.3.0. The default runner footprint pulls no
  BotResources library and is byte-identical.

## 0.4.0 - 2026-09-01

- `runner::FailureReport::kind` is now the closed `runner::FailureKind`
  (`TRANSIENT` | `PERMANENT`) instead of `Option<String>`. The offer always
  declared a two-word retry vocabulary; typing it as free text left the
  enforcement to the receiver's runtime, where the only available answer is to
  discard a terminal fact — which is exactly what happened in dev on
  2026-08-25, a runner publishing `"kind": "scaffold"` and its job stalling.
  The constraint now lives in the producer's compiler.
- Source-breaking for producers by design, wire-identical for conforming ones:
  the field keeps its name, its uppercase codes and its absence. An absent kind
  still parses and still means `PERMANENT` (`FailureKind::default()`), and a
  report that declares no kind still writes no `kind` field.
- Runner-side adoption: recompile against this version. A runner that sent a
  word outside the vocabulary now fails to build instead of shipping.
- Added `RunnerStatusFact::as_str` / `parse` and `RunnerStatusFact::ALL`
  (additive). The fact segment of a status subject is now readable from the
  crate that renders it, so a receiver dispatches on the same vocabulary it
  publishes instead of re-spelling the five words in its own consumer.

## 0.3.0 - 2026-08-27

- Added version 2 coordinates and subject constants for `job.cancel`,
  `job.finish`, and `job.fail`.
- Kept the command payload DTOs wire-identical across versions: the version 2
  change concerns admission semantics, not message shape. Version 1 constants
  and coordinates remain available to compatibility consumers.

## 0.2.0 - 2026-08-21

- Added the `jobs.runner_type.{runner_type}` Published Language key builder and
  the `catalog::RunnerType` DTO, versioned like every other wire value: a
  `version` field (wire v1) that defaults to 1 when absent. Entries carry
  `ACTIVE` or `DEPRECATED`; retirement is represented by retracting the key.

## 0.1.0 - 2026-08-17

- The offered subjects and KV key templates as constants, plus typed
  `CommandCoords`/`EventCoords` constructors for every integration subject.
- Two audiences, one crate: by default it carries the runner wire and the
  subject constants alone and depends on no BotResources library, so a runner
  binary never compiles `br-rust-common`; a platform service that publishes or
  consumes the integration subjects enables the `integration` feature, which
  adds the typed `CommandCoords`/`EventCoords` constructors.
- Payload types for the whole wire, transcribed from the sealed offers of 0.1:
  the four integration commands (`CreateJob`, `CancelJob`, `FinishJob`,
  `FailJob`), the eight integration events (`JobQueued`,
  `JobCreationRejected`, `JobStarted`, `JobPlanDeclared`, `JobStepStarted`,
  `JobCompleted`, `JobFailed`, `JobCancelled`) and the runner transport
  (`Trigger`, `RunStarted`, `PlanDeclared`, `StepStarted`, `RunCompleted`,
  `RunFailed`, `LogLine`, `CancelRun`, `Presence`) with the declared stream and
  bucket names.
- `Presence.status` is the closed enum `RunnerStatus` (`READY` | `DRAINING`),
  per the registry offer: `READY` takes new deliveries, `DRAINING` is alive and
  finishing its current runs while taking none. Any other code fails to parse,
  so a presence entry can never be read as `READY` by default.
- `Presence.capacity` is required and at least 1 (`Capacity`): a missing, zero
  or negative capacity fails to parse, so a presence entry is never read with a
  defaulted room.
- `CreateJob.producer` is required, as the offer requires it: every job names
  the producing bounded context the domain, the database and the administrator
  surface all need. A creation that omits it does not deserialize at all, so it
  takes the malformed-payload path — answered by a `creation_rejected` event
  naming the job id — instead of being attributed by guesswork. There is no
  fallback attribution and no reserved producer key: a job declared by a runner
  for its own child names its producer like any other producer does. When
  `source_bc` is present it must be that same key, or the creation is refused.
- Three shapes are supersets of the sealed offer text, each accepting a message
  that follows the offer literally: `CreateJob.triggered_by` accepts the offer's
  bare user id and also the pair `{ id, display_name }`, so a producer that
  already holds the name spares the administrator surface a lookup jobs cannot
  make; `LogLine.id` and `PlanDeclared.declaration_id` are optional so an
  at-least-once redelivery can be absorbed under the identity the runner already
  gave it, and a declaration identity outside the platform's UUIDv7 rule is
  recorded under a minted one rather than dropped.
- `RunFailed`'s retry hint is carried by the field `retry_after_seconds`, with
  the serde alias `retry_after` — the offer's name. Both spellings are accepted
  on the wire; the crate serialises the explicit one.
- The envelope's `command_id` constrains nothing beyond the envelope: the
  resolution a `job.cancel`, `job.finish` or `job.fail` opens is identified by
  an id svc-jobs mints, so a producer minting its command ids under any UUID
  version is served.
