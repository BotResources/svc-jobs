# Changelog — contract-jobs

All notable changes to the published language of svc-jobs are documented
here. This crate is the inter-BC contract: a breaking change here ripples to
every consumer, so each version is deliberate. Release headings are plain
`## x.y.z` — the release pipeline greps that exact form before tagging.

## 0.2.0 - 2026-08-20

- Added the `jobs.runner_type.{runner_type}` Published Language key builder and
  `PublishedRunnerType` DTO. Entries carry `ACTIVE` or `DEPRECATED`; retirement
  is represented by retracting the key.

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
