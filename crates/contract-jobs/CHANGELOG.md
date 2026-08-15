# Changelog — contract-jobs

All notable changes to the published language of svc-jobs are documented
here. This crate is the inter-BC contract: a breaking change here ripples to
every consumer, so each version is deliberate.

## [Unreleased]

- Scaffolded from major 0 in the Services registry: the offered subjects
  and KV prefixes as constants, no payload types yet.
- Payload types for the whole wire, transcribed from the sealed offers of 0.1:
  the four integration commands (`CreateJob`, `CancelJob`, `FinishJob`,
  `FailJob`), the eight integration events (`JobQueued`,
  `JobCreationRejected`, `JobStarted`, `JobPlanDeclared`, `JobStepStarted`,
  `JobCompleted`, `JobFailed`, `JobCancelled`) and the runner transport
  (`Trigger`, `RunStarted`, `PlanDeclared`, `StepStarted`, `RunCompleted`,
  `RunFailed`, `LogLine`, `CancelRun`, `Presence`) with the declared stream and
  bucket names.
- Three shapes are supersets of the sealed offer text, each accepting a message
  that follows the offer literally: `CreateJob.producer` names the producing
  bounded context that the domain, the database and the administrator surface
  all require and that the offer omits — a message without it is attributed all
  the same, to the source reference's bounded context, else to the runner type
  of the parent job, else to the reserved key `unattributed`; `LogLine.id` and
  `PlanDeclared.declaration_id` are optional so an at-least-once redelivery can
  be absorbed under the identity the runner already gave it, and a declaration
  identity outside the platform's UUIDv7 rule is recorded under a minted one
  rather than dropped.
- The envelope's `command_id` constrains nothing beyond the envelope: the
  resolution a `job.cancel`, `job.finish` or `job.fail` opens is identified by
  an id svc-jobs mints, so a producer minting its command ids under any UUID
  version is served.
