# Changelog — bc-jobs

All notable changes to this bounded context are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
the crate adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The version in [`Cargo.toml`](Cargo.toml) is the source of truth; release
headings are plain `## x.y.z` (the `0.0.0-dev` placeholder maps to
`## Unreleased`).

## 0.9.0 - 2026-11-02

- `domain::auto_finish::AutoFinishFacts` and its `decide`
  (`commands::job::auto_finish`): the decision that a called Job is finished by rule, from the facts of the Job, of its
  restart Jobs, of the answers its Caller read and of the Caller Runs. It
  returns `FinishByRule` (the Job, the restart Jobs to finish and the premise
  the commit must still find true) or an `AutoFinishWait` with a stable
  reason. `ResolutionRequester::Rule(FinishVerdict)` lets `Job::finish` accept
  that decision and no other actor: the verdict can only be made by `decide`.
- `ports::auto_finish::AutoFinishReader`: the facts of a Job, the Jobs a
  sweep considers (paged by Job id), the Jobs called by given Runs and the
  Jobs called from the Tree of a given restart Job.
- `commands::job::restart::refusal_ends_the_wake`: whether a refused restart
  leaves no wake behind.
- `Job::content_erased_at`: the first erasure time of the request data of a Job,
  `None` until it was erased. `JobState::content_erased_at` is the stored value; a
  Job marked erased that still holds a payload, a Passport or a configuration
  cannot be loaded.
- `JobEvent::JobContentErased` (`event_type` `JobContentErased`, fact
  `JobContentErased { job_id, at }`): the first erasure of the request data of a
  Job by the sweep. It reaches the clients and changes no other state.
- `ports::job::ContentErasure` carries the time, the event id and the metadata
  of that erasure to `CancelledRequestData::erase_request_data_of`.
- `Retention::cancelled_request_data_is_due(cancelled_at, now)` and
  `Retention::cancelled_request_data_due_before(now)`: the request data of a
  cancelled Job is due once the cancelled retention has passed since the cancel
  (at once with a cancelled retention of 0). The Jobs that wait for the erasure
  are the ones `ports::job::CancelledRequestData::cancelled_jobs_awaiting_erasure`
  returns.
- `domain::erasure::{ERASED_TEXT, ERASED_CONVERSATION_ID}`: the fixed values that
  replace the author and the source conversation of an erased mail; both pass
  the checks that load a mail.
- `UploadRefusal::RequestDataErased` (`request_data_erased`), `ExistingJob::request_data_erased`
  and `MailRefusal::MailContentErased` (`mail_content_erased`).
- `Retention::scrub_standing(&TreeFacts, now) -> ScrubStanding` and
  `Retention::scrub_due_before(now)`: the decision that a Tree is due for its
  scrub (not while a Job is unresolved or a Run is live, not before the
  retention has passed since the last activity, never twice).
- `events_of_a_scrub(&[Job], at)`: one `JobContentErased` per Job of the Tree,
  carrying the time of the scrub, or the time the request data of that Job was
  first erased when a cancel sweep had erased it earlier, and a
  `JobAffordancesChanged` for each Job whose manual retry was available.
- `ports::scrub::{TreeScrubs, TreeScrub, ScrubOutcome, OrphanUploads}`.
  `TreeScrubs::trees_due_for_scrub(due_before, batch, after)` returns
  `DueTree { initial_job_id, created_at }` values, oldest Initial Job first,
  and `after` continues a page after the last Tree of the previous page.
- `JobsError::TreeScrubbed` (`tree_scrubbed`), `Job::tree_scrubbed`: a manual
  retry, a restart, and the affordance for the manual retry, are refused in a
  scrubbed Tree.
- `JobsError::RequestDataErased` (`request_data_erased`): a restart of a Job
  whose request data was erased is refused, and the refusal ends the wake of that
  restart (`commands::job::restart::refusal_ends_the_wake`). A Job that is not
  resolved cannot carry `content_erased_at` or a scrubbed Tree: it fails to load
  with the reason `erased_job_not_resolved`.
- `domain::directory::removal::DirectoryRemoval`: the Runs, the Runs left
  without an agent because a removed restart Run took them, and the mail
  addresses that one cancel removes from the agent directory, and
  `removes_the_caller_of(&CallNotices)`, the decision that a Caller removed by
  the same cancel keeps no notice: a background Caller by its Run or its
  address, a sync Caller by its Run or its emptied Run.
- `Job::record_run_started_request`, `commands::job::run_start_request::StartRequestFact`
  and `domain::run::start_refusal::StartRefusal`: the decision of the started
  request (hold under a start id, accepted again for the same start id and
  holder, six refusals with stable codes and plain messages). `StartId` is a
  UUIDv7 id; `RunStart::start_id` and `RunStarted::start_id` carry it (`None`
  for a Run held by the status-stream fact, and for events stored before it
  existed).
- `Job::record_run_started` (status-stream fact) now discards a Run reserved
  for another Instance as `CommandWarning::FactDiscardedReservedElsewhere`
  instead of returning an error. Breaking: `JobsError::RunReservedElsewhere`
  (code `run_reserved_elsewhere`) is removed from `JobsError`; the started request
  refuses with `StartRefusal::RunReservedElsewhere` and the same code.
- `domain::mail::inbox::ItemKind::MailNotice` (database word `MAIL_NOTICE`) and
  `NoticeType::Refused`; `Notice::job_id` is optional and `Notice::mail_id` names
  the refused mail of a mail notice. `ItemKind::WAKING`, `NoticeType::WAKING`
  and `NoticeType::starts_a_restart` give the one list of the notices that start
  a restart. `Notice::of_job` and `Notice::of_mail` build a notice.
- `domain::mail::refused::mail_notice` and `MailRefusal::leaves_a_mail_notice`,
  `notice_words` and `notice_key`: which mail refusals leave a mail notice, its
  words and the key that makes a repeated refusal leave one notice.
  `ports::mail::MailSender::note_refused_mail` and `RefusedMail` write it.
- `CallNotices::plan` tells a background Caller that the called Job was
  cancelled (`NoticePlan::JobCancelled`), whoever cancelled it;
  `CallNotices::refusal_address` gives the address of a refused background
  call's notice.
- `domain::policy::Retention`: the days a Tree is kept after its last activity
  (1 to 395) and the days the request data of a cancelled Job is kept (0 to
  395, never above the first). `Retention::from_days` builds it and refuses
  a value outside the bounds with `JobsError::OutOfRange`, and a cancelled
  retention above the retention with
  `JobsError::CancelledRetentionAboveRetention` (code
  `cancelled_retention_above_retention`); `Retention::tree()` and
  `Retention::cancelled()` read it. `ServiceLimits::with_retention`,
  `ServiceLimits::retention`, `ServiceLimits::tree_retention` and
  `ServiceLimits::cancelled_retention` carry them; the defaults are 30 and 0.
  The bounds and defaults are public as `domain::policy::MIN_RETENTION_DAYS`,
  `MAX_RETENTION_DAYS`, `DEFAULT_RETENTION_DAYS` and
  `DEFAULT_CANCELLED_RETENTION_DAYS`.
- `Job::fail_run_for_terminated_trigger(ReclaimRun, &RetryPolicy, &ServiceLimits)`:
  a Run that has not started and is not reserved fails as permanent with the
  reason `trigger_terminated` (`domain::run::failure::TRIGGER_TERMINATED`), which
  ends its Job with no retry; a Run that started, a Run reserved by a claim and a
  Run that ended produce no event and a warning (`CommandWarning::
  TriggerTerminationIgnored { run_standing: IgnoredRunStanding }` (`started` or
  `reserved`), code `trigger_termination_ignored`, or the existing
  `fact_discarded_on_terminal_run`). A pushed Run that failed with that reason
  without ever starting loads.
- `ports::transport::TriggerSequence` (the stream sequence the broker gives a
  published trigger, never zero) and `ports::trigger_ledger::TriggerLedger` (store
  the sequence of a Run's trigger; find the Run a sequence belongs to).
  **Breaking for an implementor of `RunnerTransport`:** `dispatch` returns the
  `TriggerSequence`.
- `Job::repeat_of_finish`, `Job::repeat_of_fail` and `Job::repeat_of_cancel`:
  the decision that a command repeats the command that resolved the Job. The
  sender is checked first (`NotRequester`, `NotOwner` or
  `RunnerNotWorkingTheJob` as for the command itself), then the answer is the
  event of the first resolution, rebuilt from what the Job stored. A command
  id that resolved nothing, or resolved the Job another way, is no repeat.
  `JobResolution::command_id` and `JobResolution::remembering` carry the
  command id (`CommandId`, a UUID of any version) stored with a resolution.
- `CreateJob::is_the_same_declaration_as` is public. For a Job whose request
  data was erased it compares the runner type, the producer, the parent, the
  identity of the user, the Requester, the source, the maximum attempts and the
  priority, and ignores the configuration, the Passport and the payload.

## 0.8.2 - 2026-11-01

- `domain::mail::inbox::NoticeType::JobCancelled` and
  `domain::runner::call_notices::NoticePlan::JobCancelled`: a sync call tells its
  Caller's Run that the called Job was cancelled; a background call does not.
  `event::run_inbox::RunInboxEffect::JobCancelled` classifies the event, and it
  serializes with the Tree directory like the other events that write a notice.
- `domain::runner::work::WorkNamed::runner_type`: the runner type the caller
  names, when it names one; `RunStanding::StartedOnAnotherInstance` follows when
  the Instance of the key runs another type.
- `domain::directory::holder::standing_of` and `standing_of_started_run`: the
  standing of a Run from its stored facts (unknown, removed, not started,
  ended, then the holder) and the rule that tells a started Run held by the
  named Instance from one held elsewhere. The Postgres adapter calls them; the
  v3 acting path keeps its own typed check.

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
- `event::queued::QueuedTree` and `JobQueued::tree`: which Tree a queued Job
  belongs to (named by the event, below its parent, or its own), and
  `JobEvent::queued`: the queued fact of an event, by an exhaustive match.
- `ports::PortError::TreeLockTimeout`: the Tree lock stayed held past the
  bounded wait; an adapter reports it apart from `ConcurrentModification`.
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
