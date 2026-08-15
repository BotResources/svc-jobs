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
  that follows the offer literally: `CreateJob.producer` (the domain and the
  administrator surface both require the producing bounded context, which the
  offer omits); `LogLine.id` and `PlanDeclared.declaration_id`, optional so an
  at-least-once redelivery can be absorbed under the identity the runner already
  gave it; and the resolution commands carry the client-minted `id` of the
  resolution they open.
