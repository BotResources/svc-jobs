# Changelog — bc-jobs

All notable changes to this bounded context are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
the crate adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The version in [`Cargo.toml`](Cargo.toml) is the source of truth; release
headings are plain `## x.y.z` (the `0.0.0-dev` placeholder maps to
`## Unreleased`).

## 0.8.1 - 2026-10-30

- `ports::directory::TreeDirectory::entries_of_tree`: the directory entries of
  the Tree of a Job, for the administrator view.
- `ports::transport::RunnerTransport::announces`: whether one Instance has a
  presence entry, read directly.
- `event::run_inbox::RunInboxEffect` and `JobEvent::run_inbox_effect`: the one
  exhaustive classification of what an event does to the inbox of a Run (start
  or end a Run, write a call notice); `serializes_with_the_tree_directory` says
  whether its write takes the Tree lock, and `CallNotices::plan` derives the
  notices from it. A new `JobEvent` does not compile until it is classified.
- Refactor, same decisions: `decide_deliveries` handles `RestartInFlight` in the
  arm of `Running`. A restart Run that has not started is rung by nothing; the
  0.8.0 note on `Delivery::rings_live_run` means a started Run.

## 0.8.0 - 2026-10-30

- `commands::job::restart::restart_job` and `RestartJob`: the restart Job of a
  finished Run (`JobQueued.restart_of_run_id`, `restart_of_job_id`;
  `Job::restart_of_run_id`, `is_restart`).
- `RestartStanding` and `DirectoryEntry::restart_standing`: the one decision on
  the restart of an agent, shared by the listing, the delivery of a mail and the
  restart; `Delivery::starts_restart`.
- `ActsFor::runs`; `RESULT_RETENTION`, `result_expires_at`, `guard_result_alive`.
- `guard_kept_payload_uploaded`; `JobsError::PayloadNotUploaded` and
  `PayloadUploaderMismatch`.
- `domain::runner::call_notices`: `CallNotices` and `NoticePlan`, the notices a
  runner call owes its Caller.
- `Job` carries its Initial Job, `Requester` (a service actor or a Run),
  `JobPassport`, `JobPayload` and `JobCall`; `CreateJob` and `JobQueued` carry
  them; a root job without a Passport is refused (`PassportRequired`).
- `Requester::guard_resolves` / `guard_cancels` decide who finishes, fails and
  cancels; `ResolutionRequester::Caller` and `CancelRequester::Caller`; a cancel
  of an ended job by its Requester changes nothing
  (`CommandWarning::CancelledJobAlreadyTerminal`).
- New refusals: `NotRequester`, `RunnerNotWorkingTheJob`, `ActingRunMissing`,
  `ActingRunNotLive`, `PassportRequired`, `PassportMalformed`,
  `PassportUserMismatch`, `PassportParentMismatch`, `InitialJobMismatch`,
  `ParentNotOfRun`, `ModeRequired`, `CallerAddressNotHeld`, `PayloadTooLarge`.
- `domain::directory`: `AgentId`, `AgentState`, `decide_registration`
  (refusals `RegistrationRefusal::{RunUnknown, RunNotStarted, RunEnded,
  RunRemoved, InstanceMismatch, AgentIdInOtherTree, AgentIdNotFinished,
  AgentNamedTwice, Malformed}`; `AgentNamedTwice` and `Malformed` both travel
  on the wire as `malformed`) and `DirectoryEntry` with `restartable_now`.
- `JobsError::CallerAddressNotOfRun`, `AgentId`, and the `run_removed` refusal
  of a registration. An `AgentId` is any UUID: the runner makes it.
- `RunnerTransport::acknowledged_start`: the start fact of a Run that the
  broker acknowledged and Jobs has not applied yet.
- `JobsError::NoFreeCapacity`, `RunnerType::declared_capacity` and `free_capacity`.
- `ports::directory::AgentDirectory` (`register`, `list`): the decision is
  passed in, the adapter owns the transaction and the Tree lock.
- `domain::directory::caller_address`: the single decision on the address of a
  BACKGROUND call.
- Mail, notices, kept payloads, Results and saved states: the pure decisions
  `mail_id_reused`, the recipient rules, the read rules and the size rules.
- A Job that is finished or declared failed keeps its live Run: finish and
  declared failure withdraw nothing; the terminal fact of the Run applies as
  usual and leaves the Job unchanged. `Job` may hold a live Run after its
  resolution.
- `SuccessorDispatch::Wait` and `RunnerType::successor_dispatch(current_runs)`:
  a manual retry dispatches only while the type has free capacity.
- `ServiceLimits::inline_payload_max_bytes`; `RunTrigger` carries the lineage,
  the Passport, the payload and the call.
- `ExistingJob` and `decide_upload`: an upload for a Job that exists is refused
  unless its payload is of the kept form; `guard_job_serves_kept_payload`.
- `guard_sender_holds`, `guard_attachments_uploaded`, `decide_attachment_upload`
  (`AttachmentBody`, `StoredAttachment`, `AttachmentAdmission`): the mail
  decisions that were in the Postgres adapter.
- `runs_lost_with_instance` also covers the live Run of a finished Job.
- `Job::fail_run_for_missing_passport` and `PASSPORT_REQUIRED`: a Run whose Job holds
  no Passport fails permanently, and a pushed Run may settle unstarted for that
  reason. `RunTrigger::passport` is required and `RunTrigger::of_job` refuses a Job
  without one (`PassportRequired`).
- `RunRefusal`, `MarkReadRefusal`: the refusals of a call that names a Run only;
  `RunnerRefusal::ItemNotYours` is removed.
- Ports `MailSender`, `MailReader`, `AttachmentStore`, `Inbox`, `KeptPayloads`,
  `Results` and `SavedStates`, with the read models `MailView`, `MailRead`,
  `AttachmentMeta`, `UnreadItem`, `UnreadMail`, `SavedStateView`, `StoredResult`
  and the request types `WorkNamed`, `SendMail`, `Sent`, `Minting`,
  `UploadAttachment`, `Uploader`; `RefusedRestart::code` and `message`.
- `RestartObstacles::restart_job_live`, `RestartStanding::RestartInFlight` and
  `domain::directory::restart::decide_restart_wake` (`RestartWake`): the one
  decision shared by the listing, the delivery and the wake.
- `ports::restart::RestartWakes`.
- `domain::mail::attachment` holds the attachment admission decisions; the
  fixtures are split in `fixtures/`.
- `InlineAnswer::decide` and `JobPayload::inline_content_size`;
  `guard_result_alive` takes the stored expiry.
- `PrincipalFacts::guard_holds_runner_scope` and `JobsError::NotRunnerService`.
- `domain::runner::saved_state::SavedState::new(value, maximum_bytes)` owns the
  size guard of a saved state and returns `SavedStateRefusal`; the port
  `SavedStates::save_state` takes the checked `SavedState` and no maximum.
- A mail or notice reaching an agent whose restart Job is live rings the live
  Run of that restart Job (`Delivery::rings_live_run` for `RestartInFlight`).

## 0.7.0 - 2026-10-29

- `PrincipalFacts::new` takes a `PlatformRole` (`Member` < `SuperUser`)
  instead of the `is_super_admin` flag. A platform administrator is a super
  user — the admins of the platform org; `guard_platform_administrator` and
  `guard_super_user` stay two decisions, each refusing under its own code.
- The decisions on a user's maximum: `UserMaxPriority::guard_set` /
  `guard_clear` (replacing `guard_govern`), `can_set` / `can_clear` and
  `affordances` (`set_max_priority`, `clear_max_priority`). Clearing needs a
  maximum to clear: `JobsError::MaxPriorityNotSet` (`no_max_priority_set`).
  `clear_max_priority` enforces it instead of recording nothing.
- `UserMaxPriorityView` (`UserMaxPriority::view`): the level that applies,
  whether it is the default, and the caller's affordances — the projection the
  administrator's read and its live changes share.
- `UserSelection`: the users one read answers for — UUIDv7 ids, each once, in
  the order first asked for, at most 200 (refused as soon as a 201st distinct
  user is named); `UserSelection::one`.
- `MaxPriorityRevision`: the count of a user's recorded maximum changes, which
  orders a user's changes for live views.
- Port `UserMaxPriorityReader`: `load_selected` (maxima with their revision,
  one statement) and `load_one`, the single non-transactional read path of a
  user's maximum.

## 0.6.0 - 2026-10-26

- Priorities: the `Priority` value (1–5), `JobPriority` (requested,
  effective, forced) on the Job aggregate, `PriorityBasis` (capped by a user's
  maximum or the service default, or inherited from the parent) and
  `UserMaxPriority`. `ServiceLimits::with_default_max_priority` (default 3).
  `PriorityBasis::ForcedByParent`: a child under a forced parent is born
  forced at that level; `JobQueued::forced_priority` carries it.
- Commands `Job::force_priority`, `Job::force_priority_as_descendant`,
  `Job::recalculate_priority`, `set_max_priority` and `clear_max_priority`;
  facts `JobPriorityForced`, `JobPriorityRecalculated` and the `KnownUser`
  aggregate's `UserMaxPrioritySet` / `UserMaxPriorityCleared`; policies
  `forcing_cascade` (every waiting descendant, forced earlier or not) and
  `recalculate_for_user`. A pushed run failed unstarted as
  `trigger_stranded` hydrates.
- `PrincipalFacts`: the trusted facts of the caller and the three decisions
  taken from them (platform administrator, super user, own maximum).
  `Job::affordances` takes the principal and carries `force_priority`.
- Claims: `Job::reserve_run` shares the dispatch decision; `RunReservation`
  and `ClaimId`; `RunnerType::guard_claim` and `guard_push_dispatch`;
  `Job::fail_run_for_lost_claim` (`claim_lost`) and
  `Job::withdraw_stranded_trigger` (`trigger_stranded`: the run is cancelled
  without counting as an attempt — `RunTerminal::withdrawn`,
  `Run::counts_as_attempt`, `RunCancelled::withdrawn_for`; the delay counts
  from `RunnerType::claimed_since`); `Job::recall_claim`
  decides whether a repeated claim is answered or spent; a reserved run is
  cancelled through a stop request and reclaimed with its lost instance.
- Runner-type settings: `DispatchMode`, `MaxRunDuration`,
  `RunnerTypeSettings` (a type registered before 0.4 has no maximum duration
  of its own and answers to the service default), commands
  `set_dispatch_mode` / `set_max_run_duration` and their facts, and
  `successor_dispatch` (whether a manual-retry successor is pushed or waits to
  be claimed); a run carries the maximum duration it was dispatched with.
- `clear_max_run_duration` and `RunnerTypeMaxRunDurationCleared`, the
  `clear_max_run_duration` affordance (`no_max_run_duration_set`);
  `RunnerTypeSettings::dispatch_mode_since`; the `DispatchModeOfferWriter`
  port; `Job::rederive_priority` for a Job back to waiting
  (`JobPriorityRecalculated::user_id` is optional). The dead
  `RunnerType::successor_dispatch` copy is gone; the free function stays.
- Breaking for callers (svc-jobs only): `create_job` takes the user's stored
  maximum, `CreateJob` / `DispatchRun` / `ManualRetryJob` / `RunnerTypeState`
  / `RunState` / `JobState` gain fields,
  `ManualRetryStarted::run_id` is optional, `DueWorkReader` gains
  `jobs_with_lost_claims` and `jobs_with_stranded_triggers`, and
  `jobs_with_outrun_runs` takes the service default.

## 0.5.1 - 2026-10-17

- Rebuilt on the per-crate `br-rust-common` pins (`br-core-events/v0.1.0`) in
  place of the legacy workspace tag `v1.3.0`. No behaviour change. Consumed
  only inside this repository.

## 0.5.0 - 2026-09-08

- Rebuilt on `br-rust-common` v1.3.0 (from v1.2.0). No behaviour change.
- Minor rather than patch because the public API exposes `br_core_events` types
  (`UserId`, `EventMetadata`, `DomainEvent`, `Actor`) in its ports and domain
  references, so the `br-rust-common` they resolve against is part of this
  crate's contract. Consumed only inside this repository.

## 0.4.0 - 2026-09-01

- `RunFailureKind::from_declared` now takes `Option<RunFailureKind>` and is
  infallible. The rule it carries — an unspecified failure kind is permanent —
  is a domain rule and now runs on domain types alone; the wire vocabulary is
  closed by the contract crate, so there is nothing left for it to parse.
  `from_db_str` is unchanged and keeps guarding stored values.
- Breaking for callers of `from_declared` (svc-jobs only): the caller maps the
  wire kind to the domain kind with a total match, so a kind added to the
  contract is a compile error rather than a value the domain rounds off.

## 0.3.0 - 2026-08-27

- Resolution decisions now distinguish a legacy owner claim from an admitted
  declarant. Legacy claims retain the owner-coherence guard; declarants are
  checked only against the Job lifecycle invariants.
- Cancellation accepts the same admitted-declarant origin while preserving
  the administrator and legacy-owner paths through one exhaustive requester
  guard.

## 0.2.0 - 2026-08-21

- RunnerType now owns a total `ACTIVE`/`DEPRECATED`/`RETIRED` lifecycle and
  granular lifecycle facts.
- One decision API drives the lifecycle command guards and the `deprecate`,
  `reactivate`, and `retire` affordances. Retirement consumes explicit current
  usage facts; reactivation from retired consumes live presence.
- Dispatch and job acceptance fail closed for retired types while presence is
  still recorded without implicit reactivation.

## 0.1.0 - 2026-08-17

- The Job and RunnerType aggregates, their commands, events, affordances and
  policies, with the double-barrier invariants.
- A runner instance's reported status is the closed `ReportedStatus`
  (`READY` | `DRAINING`); a runner type is available only while at least one of
  its live instances is `READY`, so an all-draining type blocks dispatch while
  the runs its instances already carry keep going untouched.
- An observed presence loss names the session it was observed on: closing a
  session the instance no longer holds is refused (`stale_loss`). When the
  observation carries the session itself — the backstop, which reconciles the
  bucket against the sessions it read — a loss that finds the process
  reconnected under a new session cannot close it or orphan the runs it is
  carrying. The watch path carries no session on the wire, so it pins the
  session it reads from the fleet on its first attempt: a pod delayed between
  that read and its write can still close a session that replaced the one it
  observed, in a narrow multi-pod window. Naming the session on the wire (a
  runner boot identity in the presence entry) is the full fix, and it is a 0.2
  contract change.
- The reclaim policy is session-scoped: `runs_lost_with_instance` takes the
  lost session (the instance and the window it was connected for) and names
  only the runs started inside that window, so a run the replacement session is
  executing is never reclaimed with its predecessor. The fleet ports read what
  that needs: the open sessions, and the closed session's window.
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
