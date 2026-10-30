# Changelog — svc-jobs

All notable changes to this service are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
the service adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

The version in [`Cargo.toml`](Cargo.toml) is the source of truth. CI gates every
PR on `scripts/check-changelog.sh`: whatever version sits in `Cargo.toml` must
have a matching plain `## x.y.z` heading here (the `0.0.0-dev` placeholder maps
to `## Unreleased`).

## 0.6.1 - 2026-10-30

### Fixed

- Administrators could not see the agent directory of a Tree, which the
  sealed 0.6 gives them: `jobsTreeAgents(jobId)` reads it for the Tree of a
  Job, as the runner listing shows it (the running agents and the finished
  agents that can restart). It is a snapshot; no live delta carries it.
- The presence backstop no longer reports a live Instance lost: the bucket
  listing it compares with the open sessions can miss a key rewritten while it
  is read, and each absent key is now confirmed by a direct read before its
  session is closed and its Runs are reclaimed.
- A Run start, a queued Job, a scheduled retry and a failed Job now take the
  Tree lock as a Run end, a mail and a cc notice already did, because each of
  them can put a call notice in the inbox of a Caller's Run. An item that
  reaches an agent whose restart Run starts at the same moment is either rung
  to that Run or listed by the first read the Run makes after its start; before,
  it could be neither (`regression_ring_start_race`). One classification of what
  an event does to the inbox of a Run (`bc-jobs` `RunInboxEffect`) decides both
  the lock and the notices, and a new kind of event does not compile until it
  is classified.
- Waiting for the Tree lock is bounded (5 s), instead of holding a connection
  and the status consumer for as long as another write holds the Tree. A wait
  that ends is a distinct outcome of the runner surface, not a typed refusal:
  a GraphQL runner gesture answers a top-level error with `data: null` and
  `extensions {code: "CONFLICT", reason: "concurrent_modification"}` after
  about 5 s (every typed refusal answers `{success, reason, message}`); a NATS
  surface request (`register_agents`, `list_agents`, `mark_read`) gets no
  reply, so the runner's own request times out; a runner status fact is
  redelivered until the Tree is free. Within one gesture or one delivery a
  timeout is not attempted again, so a gesture waits 5 s once, not once per
  attempt. Each timeout
  is logged at warn and counted in `jobs_tree_lock_timeouts_total`. An event
  that ends a Run or writes a call notice on a Job whose Tree is unknown is
  refused (`job_not_found`) instead of going unlocked.
- A Run whose start fact was acknowledged but not yet applied is no longer
  answered `run_not_started` when the start waits for a held Tree: the gesture
  answers the conflict above, or goes on once the start is applied.
- `jobsTreeAgents` answers `job_not_found` for an unknown Job from the
  application, as every other gesture does, instead of from the resolver.

### Added

- The gauge `jobs_claims_held{runner_type}`: the claims this pod has registered
  to wait for work.

### Known gaps

- A GraphQL runner call names its Instance by key alone and carries no runner
  type, so an Instance of another runner type with the same key is not told
  apart from the Instance that holds the Run; the NATS surface names the type
  and refuses it (`e2e_runner_isolation`,
  `a_graphql_call_of_another_runner_type_with_the_same_instance_key_is_not_refused_known_gap`).
- The queue position of a call notice is measured while the Tree lock is
  held, because the notice is written in the same transaction as the Job that
  is queued: the lock cannot be released before the write ends. The cost is one
  windowed query over the waiting Jobs of the runner type per queued call, so it
  grows with the queue.

### Tests

The end-to-end suite now proves, on real Postgres and NATS, the clauses of the
sealed 0.6 tests that it proved only in part:

- A Result is kept for 7 days, erased on read, and read only by its addressee:
  a read by reference erases it, a read with keep is read again by the Run of
  the restart Job, and a later keep read moves the expiry forward.
- A Run that ends after its Job was finished still ends as usual: no retry
  starts, and a second fact for the ended Run is acknowledged and discarded.
- A Run that fails or is cancelled cancels the Jobs it created: executing,
  waiting, a restart Job and a manual-retry successor of the failed Run; a
  terminal child stays and leaves the directory; the retry keeps its own.
- A background answer comes by mail and no Result is kept: the mail reaches the
  named address and a read of a Result is refused.
- A background runner call confirms at once and sends notices: the queue
  position is sent each time the Job starts to wait, not at each change; the
  Job is not finished by its Run; a refused call creates no Job; cancelling the
  Caller's Job cancels the called Jobs.
- A bad recipient refuses the whole mail: a removed, an unknown and an
  unrestartable address, each with its reason.
- A cc recipient gets a notice, not the mail: attachments are fetched apart
  (`jobsAttachment`), and the cc notice is read when the agent next starts.
- A direct mail wakes a finished agent as a restart Job: the work carries no
  state, agent id or mail; the old Job is untouched; a later mail starts a new
  restart Job.
- A failed or cancelled Run removes everything that depends on it: saved
  state, unread notices and addresses; a mail to a removed agent is refused; the
  cancel of a terminal Job removes its Runs and cancels what lives below.
- A failed restart Run leaves the agents finished and the mail unread: only
  what the restart Run added leaves; a cancel of the Tree removes the agent.
- A failure notice wakes a finished background Caller: the restarted Caller
  reads it; a cc notice starts nothing.
- A failure report tells an agent what to decide: the message names the Job,
  the Run, the reason and that the retries are spent; `DECLARED_BY_OWNER`.
- A mail id cannot be reused for another mail: subject, body, recipients and
  attachments, and the inbox is unchanged.
- A mail names its author and its source conversation (author length,
  blank author, UUIDv7 conversation, cc notice without either).
- A payload kept by Jobs is uploaded before its Job exists, by a runner; and a
  second upload for a Job id keeps the first payload.
- A read marker keeps unread mail across a crash, in order of acceptance, for
  mail and notices.
- A restart Job keeps the parent and the Requester of its origin: Passport,
  config, payload, mode, caller address, no predecessor, no source, forced
  priority; the restart of an Initial Job is its own parent and is capped again.
- A retired type leaves the mail unread with a reason; a terminal parent
  refuses the mail and keeps a failure notice unread.
- A retry gets new agents and a restart keeps its agent ids.
- A runner uses only its own Runs, addresses and Jobs, and is refused when it
  names another Instance's key and without the runner scope.
- A sync runner call ends with the answer or the failure in the Caller's Run,
  stays in progress until the Caller finishes it, and is cancelled with it.
- Acts-for is transitive across two restarts; a plain retry gets none of it.
- Administrators do not read mail, Results or kept payloads, and see the agent
  directory of the Tree.
- Agent registration is answered and fail-closed: a refused registration
  registers nothing; an ended Run registers no agent.
- An item is announced to the Run and nothing is lost on reconnection: cc
  notices and call notices ring as mail does.
- Cancel comes from the Requester or the working runner and reaches every Job
  below; cancelling a restart Job reaches the Jobs of the Runs it acts for.
- Claim reservation waits for free capacity after a heartbeat lowers it.
- Config and payload travel apart (the config as the same JSON value), to a
  manual-retry successor too, and a reference is not read.
- Every Job has a Passport that Jobs stores and hands on unchanged: the whole
  Passport in every trigger, the implied KnownUser, `passport_user_mismatch`.
- Mail stays inside one Tree: `other_tree`, `sender_not_of_run`,
  `sender_unknown`.
- Only the Requester finishes or fails a Job, and not through an ended Run.
- Push dispatch waits for free capacity: a second Instance frees a slot, an
  expiring one publishes nothing, a draining one adds no capacity.
- Saved state: the last one counts, it has a size limit, and a restart without
  a new state keeps the earlier one.
- The Tree is always known: `initial_job_mismatch`, and every event names its
  Job, parent and Initial Job.
- The agent list shows the own Tree: running and restartable agents.
- Unread mail at the end of a successful Run starts the restart at once and is
  delivered unread; a cc notice starts nothing.
- The doorbell rings are counted by the JSON path of their payload
  (`event_type`, `run_id`, `item_id`), no longer by a text pattern.
- The tests of the backstop sweep live in `app/backstop/tests.rs` and record a
  step when its future runs.

## 0.6.0 - 2026-10-30

Requester, Passport and lineage on every job, the runner surface, the agent
directory, the restart of finished agents, the acts-for rule, the cascade to
successors, the expiry of Results, the notices to Callers and the storage of
kept payloads.

### Added

- Restart: a direct mail to a finished agent that holds a saved state starts a
  restart Job (producer `jobs`) that keeps the parent, the Requester, the
  Passport, the config, the payload, the mode and the caller address of its
  origin and stays in the Tree of the origin, an Initial Job included. A cc
  notice never starts a restart. A failure notice to a finished background
  Caller, and the end of a successful Run that leaves mail unread, start it too.
  The start is durable (`restart_wakes`), taken once under the Tree lock, and
  processed after the commit and by the backstop sweep. A retired runner type
  leaves the mail unread and the reply names the refusal; a terminal parent
  refuses the mail. A failed restart Run puts the agents back to `FINISHED` in
  the Run they came from. Migration `0008`: `directory_agents.restarted_from_run_id`,
  `restart_wakes` and the index of the restart links.
- The acts-for rule: a restart Run acts for the Runs it restarts, transitively,
  for finish, fail and cancel of their Jobs, for the Results and for the unread
  items addressed to them. A retry of the same Job does not.
- A restart Run reads the saved state of the nearest Run of its chain; a
  successful restart Run that saves its own state replaces the earlier states.
- The cancel of a Job reaches its descendants, its restart Jobs, its
  manual-retry successors and, for a restart Job, the Jobs created by the Runs
  it acts for; a cancel of a terminal Job reaches the same non-terminal Jobs and
  removes the same Runs from the directory.
- A Result is kept for 7 days from its keeping, or from a read that asked to keep
  it; the sweep erases the expired ones.
- Notices to the Caller of a runner call: accepted, queue position (each time the
  Job starts to wait), started, run completed (with the answer when it fits
  `JOBS_PAYLOAD_INLINE_MAX_BYTES`, and a reference to the Result) and job failed
  (with the report and a message). A sync call tells only the end and the
  failure, to the Run of the Caller. They ring the doorbell of the live Run and of
  the live restart Runs that act for it.
- A creation that names a kept payload is refused with `payload_not_uploaded` or
  `payload_uploader_mismatch`; a restart or a retry fetches the payload of its
  origin.
- The chart carries `savedState.maxBytes` (`JOBS_SAVED_STATE_MAX_BYTES`).

### Changed

- Only an active service account that holds `jobs:runner` reaches the runner
  surface; a human, a super user included, reaches nothing of it.
- `/graphql` no longer carries the implicit 2 MB body limit of the HTTP layer: a
  large saved state, mail or payload is refused by its own limit.
- The scope declaration labels are `jobs.service.label`,
  `jobs.service.description`, `jobs.scope.runner.label` and
  `jobs.scope.runner.description`.
- `RUNNER_SURFACE` is built from `contract_jobs::graphql_roots`.

- `job.create.v3`, `job.finish.v3`, `job.fail.v3`, `job.cancel.v3` consumers
  (durables `svc_jobs_job_{create,cancel,finish,fail}_v3`).
- Every job stores its Passport, Requester, payload and call. The first job of
  a Tree is its own parent and Initial Job; a child takes its parent's Initial
  Job and Passport.
- `queued`, `creation_rejected`, `started`, `plan_declared`, `step_started`,
  `completed`, `failed`, `cancelled` events in version 2 (parent, Initial Job,
  Requester, plain-words `message`), published beside the version 1 events;
  `command_rejected.v1` and `cancel_accepted.v1`.
- The trigger and the claim reply in version 2, on `jobs.trigger.v2.{runner_type}`
  and `jobs.claim.v2.{runner_type}`.
- A Run that fails or is cancelled cancels the Jobs it requested.
- Migration `0004`: `requester`, `passport`, `payload`, `call` on `jobs`.
- Capacity enforcement: a Run is pushed or claimed only while its runner type
  has free capacity (the declared capacity of its ready live instances minus
  its Runs that have not ended); otherwise it waits, and the end of a Run wakes
  it. Refusal `no_free_capacity`. Migration `0005`: `current_runs`.
- The agent directory: `jobs.runner.register_agents.{runner_type}` and
  `jobs.runner.list_agents.{runner_type}` answer runner requests on the core
  bus (queue group `svc_jobs_runner_surface`), fail-closed with a typed reason.
  A Run that ends with success makes its agents `FINISHED`; a Run that fails or
  is cancelled, and the cancel of a Job, remove the agents of the Run and of its
  descendant Runs for good. Migration `0006`: `directory_agents`,
  `directory_removed_runs`, `jobs.parent_run_id`.
- A BACKGROUND call is refused with `caller_address_not_held` or
  `caller_address_not_of_run` unless the address is an inbox address of the
  acting Run in the directory.

- Before a runner-surface request is refused with `run_not_started`, Jobs
  applies the start fact of that Run that the broker already acknowledged, so a
  runner may ask at once after it published the start. Jobs sends no reply
  when it fails on its own side.
- Migration `0007`: `agent_id` of the directory is any UUID; the removed
  addresses; the mails, their recipients and attachments; the inbox items
  (mail, cc notice, notice); the kept payloads; the Results; the saved states
  and the restart origins.

- The GraphQL runner surface for holders of the scope `jobs:runner`:
  `jobsUploadPayload`, `jobsPayload`, `jobsUploadResult`, `jobsReadResult`,
  `jobsUploadAttachment`, `jobsAttachment`, `jobsSendMail`, `jobsUnread`,
  `jobsMail`, `jobsSaveState`, `jobsSavedState`. Admission has a third branch:
  a request selecting runner fields only is admitted for an active principal
  holding the scope; administrators read no mail, Result or payload.
- The scope `jobs:runner` is declared to the identity service at boot
  (`SCOPE_DECLARATION_ENABLED`, default on); readiness waits for acceptance.
- `jobs.runner.mark_read.{runner_type}` marks items read for the Run that holds
  them; the doorbell `run.item_arrived.v1` rings for an item that reaches a
  live Run.
- The Job detail carries `initialJobId`, `passport`, `requester`, `call` (mode
  and caller address) and `payload` (the inline content, the form of a payload
  kept by Jobs, or the form and the reference of a payload kept elsewhere), in
  the query and in the subscription deltas.
- `JOBS_SAVED_STATE_MAX_BYTES` (default 1 MiB).
- Registration, listing and read-marking requests are bounded in flight by
  `JOBS_CLAIM_MAX_HELD`; a request of another wire version is answered
  `malformed`; the reason `run_ended` answers a request for a Run that ended.

### Changed

- Push dispatch selects the waiting Jobs within the free capacity of each
  runner type, so a saturated type neither starves the others nor keeps the
  dispatch loop spinning.
- Finishing a Job or declaring it failed no longer withdraws its live Run and
  changes nothing in the directory. A terminal fact for a live Run of a
  terminal Job ends the Run for its Instance and for the directory and leaves
  the Job as it is; the backstops also reclaim the Runs of terminal Jobs.
- A manual retry waits for free capacity like any other Job.
- The end of a Run that fails or is cancelled also cancels the non-terminal
  descendants of the terminal Jobs it requested.
- A change of the directory takes the lock of its Tree before the Job rows, on
  the writer paths as well as on registration, so a registration cannot insert
  agents of a Run that a concurrent writer removes; the capacity locks of one
  commit are taken in runner-type order.
- Removal marks only the Runs that have not ended as removed.
- The caller-address rule is one domain decision fed with the addresses the
  acting Run holds.
- A creation of a job with no parent, by version 1 or 2, is refused with
  `passport_required`.
- A creation under an ended parent is refused with `parent_terminal` in the
  version 2 rejection (version 1 keeps `parent_job_terminal`).
- Rejections are published in both generations (`creation_rejected.v1` and `.v2`).

### Fixed

- A mail or a cc notice reaching an agent whose restart Run has started rings
  that Run once; a restart Run that has not started is rung by nothing.
- The reconciliation of the presence bucket reads the open sessions before the
  announced instances, so a session opened during the listing is not reported
  lost.
- The backstop sweep is a fixed ordered list of independent steps; the due
  restarts come last, after every safety backstop, and a failing step never
  stops the others.
- A root Job is no longer its own parent when the restart obstacle
  "parent Job terminal" is read for the listing and for mail.
- A Job that finished while its Run was still live: the Run is now reclaimed when
  it overruns its maximum duration (also after the Job was deleted) and failed
  with its instance when the runner is lost; its capacity slot and its directory
  entries are released.
- A payload cannot be uploaded for a Job that already carries an inline or a
  reference payload (`payload_already_uploaded`), and a Run fetches a kept
  payload only when its Job has the kept form (`no_kept_payload`).
- Marking items read checks only the requested items.
- Saving a state takes the Tree lock, so a save no longer leaves a state behind
  a Run that was removed at the same time.
- Two uploads of the same attachment id, or two mails of the same mail id, from
  two Trees at once keep one content and one uploader; the other is refused with
  `mail_id_reused`.
- The caller address of a background call is checked again under the Tree lock
  of the calling Run in the transaction that creates the Job.
- The runner claim is read as `ClaimV2`; a Job whose Passport is missing is not sent
  to a runner, neither pushed nor served by a claim: its Run fails permanently
  with `passport_required`. This includes a Job stored before 0.6, which holds no
  Passport.
- The bus refusal of `mark_read` is a `MarkReadRefusal`, which cannot hold
  `job_not_of_run`.
- The runner calls reach the store through the `bc-jobs` ports; the GraphQL
  resolvers of the runner surface no longer import the Postgres adapter, and the
  mail result types are split from the content result types.
- The restart of a finished agent is decided once: the listing, the delivery of
  a mail and the wake read the same facts (including "a restart Job of this
  agent is live") through `decide_restart_wake`. While a restart Job is live the
  agent is not offered as restartable, a new mail is accepted and waits unread,
  and no second restart is started. A wake for a retired runner type or a
  terminal parent is no longer queued.
- A restart wake that cannot be served no longer stops the backstop sweep: the
  safety backstops (run duration, lost claims, stranded triggers, inactivity)
  run first, the purge of expired Results and the restart wakes follow
  independently, each failed wake stays queued and the next one is tried, and a
  mail that is stored never fails because its restart could not start. Every
  step of the sweep runs whatever an earlier one returned; the sweep reports the
  first failure after all of them ran.
- The saved state and the kept payload of a restart chain are found however
  many restarts the chain holds; the 64-link ceiling is gone.
- The presence reconciliation reads the open sessions before the presence keys,
  so a session opened in between is not reported lost.
- The answer of a synchronous call rides in the notice by the same measure as an
  inline payload (`InlineAnswer`).
- The runner scope is decided by `PrincipalFacts::guard_holds_runner_scope`; the
  GraphQL edge only reads whether the scope is held.
- The fake runners of the end-to-end suites keep refreshing their presence after
  a failed refresh, and the contended scenarios run on a multi-thread runtime.
- Migration `0009_result_expiry`: `job_results.expires_at` (backfilled from the
  last read or the keeping, plus seven days) and its index; the purge is a
  range delete on it.

### Removed

- The claim subject `jobs.claim.{runner_type}` and the trigger subject
  `jobs.trigger.{runner_type}` are no longer served or published.

## 0.5.1 - 2026-10-29

Background NATS consumers recover from a bus outage of any length, on their
own. No change to the GraphQL surface, the published contract or the schema.

### Fixed

- A supervised background task (the command consumers, the runner status,
  log and presence consumers, the claim responder, the outbox relay…) was
  restarted at most `JOBS_TASK_RESTART_BUDGET` (10) times in a row, then given
  up for good. Any NATS outage longer than that — about two minutes on the
  shipped backoff — left the pod NOT READY until an operator restarted it,
  while `/livez` kept answering, so Kubernetes never did: no job was created,
  cancelled, completed or dispatched in the meantime. A task is now restarted
  for as long as the process lives; once the bus is back every task binds
  again and `/readyz` turns ready by itself. Readiness keeps its meaning (a
  task is down from the end of an attempt until it binds again) and `/livez`
  stays independent of the bus.

### Changed

- Each restart wait is drawn within `[backoff / 2, backoff]` (still doubling
  from `JOBS_TASK_RESTART_INITIAL_BACKOFF_MILLISECONDS` and capped at
  `JOBS_TASK_RESTART_MAX_BACKOFF_SECONDS`), so the tasks one outage drops do
  not hit the bus back in the same instant. An attempt that lived
  `JOBS_TASK_STABILITY_SECONDS` still starts the backoff over.
- A lost task is logged once per attempt at `warn` (task, attempt, next delay,
  cause; `error` for a panic), and its recovery at `info` with how long it was
  down.

### Added

- Metrics `jobs_background_task_up{task}` (1 while a supervised task is bound
  to its source of work, 0 while it is down) and
  `jobs_background_task_restarts_total{task}`, both exposed from boot.

### Removed

- `JOBS_TASK_RESTART_BUDGET` is no longer read. A deployment that still sets
  it is unaffected; drop it from its environment.

## 0.5.0 - 2026-10-29

Administrators read users' maximum priorities. Additive: every 0.4 behaviour
is kept, except the refusal of a clear that has nothing to clear (below).

### Added

- `jobsUserMaxPriorities(userIds: [UUID!]!): [JobsUserMaxPriority!]!` —
  platform administrators read, for the users they name, the maximum priority
  that applies to each: `maxPriority`, and `isDefault` when it is
  `JOBS_DEFAULT_MAX_PRIORITY` rather than the user's own. A user Jobs has
  never observed reads at the default. Each user is answered once, in the
  order first asked for; at most 200 users per read (`value_too_long`), and a
  user id that is not a UUIDv7 refuses the read (`not_uuid_v7`). There is no
  listing of every user.
- Each entry carries the caller's affordances `set_max_priority` and
  `clear_max_priority`, decided by the very guards `jobsSetUserMaxPriority`
  and `jobsClearUserMaxPriority` enforce: both are super-user gestures
  (`not_super_user` otherwise), and clearing is blocked
  `no_max_priority_set` (params `userId`) while the user has no maximum of
  their own.
- `jobsUserMaxPrioritiesChanged(userIds: [UUID!]!)` — the same read as a
  subscription: a `JobsUserMaxPrioritiesSnapshot` equal to the query, then a
  `JobsUserMaxPriorityDelta` carrying the complete new view of a watched user
  each time their maximum changes. Changes of unwatched users, a change that
  leaves the view as it was, and a change no later than the view already sent
  (ordered by the user's revision) push nothing.

### Changed

- `jobsClearUserMaxPriority` on a user who has no maximum of their own is
  refused `no_max_priority_set` (`INVALID_STATE`, params `userId`) instead of
  being acknowledged as a no-op, so the mutation and its affordance agree —
  as `jobsClearRunnerTypeMaxRunDuration` already does with
  `no_max_run_duration_set`.

### Fixed

- `jobsMyMaxPriorityChanged` could push a stale maximum right after its
  snapshot: a change committed before the snapshot was read but delivered
  after it. The snapshot now carries the user's revision and a change no later
  than it is dropped.

## 0.4.0 - 2026-10-26

Priorities and claim-based dispatch. Every 0.3 behaviour of a `PUSH` runner
type is kept; what changes for it is the order its waiting jobs are published
in.

### Deprecated

- The `PUSH` dispatch mode and the `jobs.trigger.{runner_type}` stream: kept
  while runners migrate to the claiming toolkit, removed in a later minor. The
  GraphQL value `JobsDispatchMode.PUSH` carries the deprecation.

### Added

- **Priorities.** A job requests a level from 1 (lowest) to 5 (highest), 3 by
  default, and waits at an effective level capped by the maximum of the user
  it runs for — or by `JOBS_DEFAULT_MAX_PRIORITY` (default 3) when that user
  has none or the job runs for nobody. A child job requests and waits at its
  parent's effective level. A manual-retry successor copies its predecessor's
  request, capped again (a forced level is not copied).
- Super-user mutations `jobsSetUserMaxPriority`, `jobsClearUserMaxPriority`
  and `jobsForceJobPriority`. Setting a maximum accepts any user id, observed
  by Jobs or not. A change of a user's maximum recalculates their waiting,
  non-forced root jobs and the waiting, non-forced descendants inheriting from
  them; forcing a job also forces every waiting descendant, forced earlier or
  not (the latest force on an ancestor wins). A
  descendant reached by either cascade records the inherited level as both its
  request and its effective level. A forced level holds until forced again.
- The user surface: `jobsMyMaxPriority` and `jobsMyMaxPriorityChanged`
  (snapshot, then every change), open to any active human and to nothing else.
- Job views carry `requestedPriority`, `effectivePriority` and
  `forcedPriority`; job affordances carry `force_priority`; job streams carry
  `JobsJobPriorityForcedEvent` and `JobsJobPriorityRecalculatedEvent`.
- **Dispatch order.** Highest effective priority first; within a level, strict
  round-robin between root jobs (least recently served root first, then the
  root whose oldest waiting job is oldest); then creation time. Both modes.
- **`CLAIM` dispatch mode** per runner type: instances claim their next run on
  `jobs.claim.{runner_type}` (core NATS request/reply, queue group
  `svc_jobs_claims`, served by every pod). A claim is held open up to
  `JOBS_CLAIM_MAX_WAIT_SECONDS` (default 30); the job is locked with
  `FOR UPDATE SKIP LOCKED` and its run created reserved to the claiming
  instance before the reply leaves. A claim repeated under the same `claim_id`
  is answered with the run already reserved. A reserved run not started within
  `JOBS_CLAIM_RESERVATION_SECONDS` (default 60) fails as transient with
  `claim_lost` and gets a cancel entry. A waiting job of a `CLAIM` type is
  cancelled in the ledger only; a reserved run is told to stop through the
  cancel bucket.
- **Per-runner-type settings**: `jobsSetRunnerTypeDispatchMode` and
  `jobsSetRunnerTypeMaxRunDuration` (platform administrators), the
  `dispatchMode` and `maxRunDurationSeconds` fields, the `set_dispatch_mode`
  and `set_max_run_duration` affordances, and the
  `RUNNER_TYPE_DISPATCH_MODE_CHANGED` / `RUNNER_TYPE_MAX_RUN_DURATION_CHANGED`
  fleet event kinds. A change applies to runs dispatched afterwards: a run
  carries the maximum duration it was dispatched with.
- `integration.cmd.jobs.job.create.v2` on its own durable
  (`svc_jobs_job_create_v2`): v1 plus an optional `priority`; out of range is
  rejected with `invalid_priority`. v1 is unchanged and requests 3.
- A child admitted under a forced parent is born forced at the parent's forced
  level (requested, effective and forced alike).
- Each pod holds at most `JOBS_CLAIM_MAX_HELD` (default 1024) claims open and
  answers a claim beyond that `busy` with `retry_after_ms`. A queued job wakes
  one held claim of its type; the pod keeps one retry timer per type, which
  wakes one held claim when the earliest retry falls due.
- A closed set of seven claim refusals: `instance_unknown`, `not_claim_mode`,
  `instance_draining`, `runner_type_retired`, `claim_spent`,
  `malformed_claim` (a nil `claim_id` is malformed) and `busy` (the pod could
  not hold the claim, or its store failed: repeat the same claim after
  `retry_after_ms`). An infrastructure error on the claim path is answered
  `busy`, never with an empty reply and never `instance_unknown`.
- Backstop `trigger_stranded`: a run published as a trigger, neither started
  nor reserved `JOBS_CLAIM_RESERVATION_SECONDS` after its type switched to
  `CLAIM`, is cancelled without counting as an attempt — its job waits to be
  claimed, the claimed run takes the same attempt number, no retry is
  scheduled. `JobsRun.cancellationReason` names the reason.
- `jobsClearRunnerTypeMaxRunDuration` returns a type to the service default,
  with the `clear_max_run_duration` affordance (blocked with
  `no_max_run_duration_set` while the type has none of its own) and the
  `RUNNER_TYPE_MAX_RUN_DURATION_CLEARED` fleet event kind.
- The KV offer `jobs.dispatch_mode.{runner_type}`
  (`{version, runner_type, dispatch_mode}`) for every active or deprecated
  type, written after each switch and reconciled at boot and periodically. The
  `jobs.runner_type.` entry is unchanged.
- A job that goes back to waiting (a retry, a withdrawn trigger) re-derives its
  effective priority from the current cap or its parent; a forced job keeps
  its force. A change of `JOBS_DEFAULT_MAX_PRIORITY` applies to new admissions
  and to these re-derivations only.
- Migration `0003_priority_and_claim.sql`. Existing jobs read as requesting
  and waiting at 3 (column defaults, no table rewrite: checks and the root
  foreign key are added `NOT VALID`, then validated). Existing runner types
  read as `PUSH` and store no maximum duration of their own: they show and
  apply `JOBS_RUN_MAX_DURATION_SECONDS`. Existing runs answer to the service
  default. New tables `user_max_priorities`, `run_reservations`,
  `waiting_jobs`, `root_dispatch_turns` and `unstarted_pushed_runs`. The
  uniqueness of a job's attempt numbers and of a retry schedule's consumer now
  holds among the runs that count (partial unique indexes), and `job_states`
  counts those runs only.
- The access paths `waiting_jobs` and `unstarted_pushed_runs` are filled and
  repaired from the ledger at boot and on every backstop sweep, which covers
  the facts a 0.3 pod records during a rolling update. The drift is read
  without locks; only drifted jobs are locked and fixed, so the sweep never
  makes a claim skip a correct waiting job. Pre-0.4 lineages find
  their root by walking their parents; `jobs.root_job_id` is not rewritten.

### Changed

- `JOBS_RUN_MAX_DURATION_SECONDS` is the maximum of every runner type that has
  none of its own, not a single fixed maximum. Keep the `JOBS_CANCEL`
  bucket TTL above the largest maximum any type is set to.
- The run-duration backstop holds each run to the maximum it was dispatched
  with.
- `JobsManualRetryStartedEvent.runId` is nullable: the successor of a `CLAIM`
  type has no run until it is claimed. `JobsRun.instance` names the reserving
  instance of a claimed run before it starts; `JobsRun.reservedAt` and
  `JobsRun.maxRunDurationSeconds` are new.
- The GraphQL door admits an active human who is not a platform administrator
  for the user surface alone; every other request of theirs is still refused
  `FORBIDDEN` before a resolver runs.
- `job.queued.v1` is unchanged.
- No preemption: a started run is never interrupted for a higher priority.
- A cancellation re-reads, under its locks, the descendants it decided on and
  starts over when the tree grew meanwhile.
- Admission re-checks the priority facts it decided on under a shared lock on
  the user or on the root of the parent's tree and then the parent; a parent
  settled in the meantime refuses the child with `parent_job_terminal`.
  Forcing and a change of maximum lock what they reach before deciding each
  row. Every writer locks the root of what it touches first, then the rest by
  id.
- Claims consider only the head of each (root, level) and lock the waiting
  row, then the job row, with `SKIP LOCKED`; claim timing uses the database
  clock. The push pass reads the same index in order.
- The list and job subscriptions take facts in batches (one store read per
  batch) and send a fresh snapshot when a batch carries more than 64 facts
  for them; the job tree is read once per batch, whole — never patched from
  the facts in hand, since one transaction's facts may span two batches. A
  fact-triggered push pass waits `JOBS_DISPATCH_MINIMUM_WAKE_MILLISECONDS`
  after the previous one.
- Startup refuses `JOBS_CLAIM_RESERVATION_SECONDS` not above
  `JOBS_CLAIM_MAX_WAIT_SECONDS`.

### Known limitations

- Switching a runner type from `PUSH` to `CLAIM` does not withdraw triggers
  already published; one consumed by a remaining push instance runs, one left
  unstarted is cancelled as `trigger_stranded` and its job is claimed.
- `jobs.trigger.{runner_type}` carries the deprecation intent of the contract;
  no deletion version is set yet.
- `contract-jobs` 0.6.0, `bc-jobs` 0.6.0.

## 0.3.4 - 2026-10-17

Dependency re-pin maintenance release. No wire, schema, migration or
configuration change for consumers.

- Every `br-rust-common` dependency moves from the legacy workspace tag
  `v1.3.0` to the per-crate tags `{crate}/v{version}` (`br-util-postgres`
  `0.1.1`, the others `0.1.0`). `br-test-harness` moves to
  `br-test-harness/v0.1.1`. The workspace resolves one copy of each `br-*`
  crate, matching `br-service-engine` 0.4.2.
- `br-util-postgres` 0.1.1 validates the database URL from the authority only
  and refuses a URL with no scheme, a foreign scheme or no explicit host.
  `ensure_app_role` refuses an empty password (a `JOBS_APP_PASSWORD` set but
  empty now fails the boot instead of clearing the `jobs_app` password), stores
  a SCRAM-SHA-256 verifier, and role creation and grants run under advisory
  locks. A deployed `DATABASE_URL` / `DATABASE_URL_OWNER` must name its host
  explicitly.
- `contract-jobs` and `bc-jobs` move to 0.5.1.
- Lockfile: `rustls` 0.23.46 and `quinn-proto` 0.11.19 (security advisories).

## 0.3.3 - 2026-09-28

Security release, lockfile bump only. No source, wire, schema, migration or
configuration change for consumers.

### Security

- `cargo update -p rustls-webpki -p rustls`: rustls 0.23.43 → 0.23.45,
  rustls-webpki 0.103.14 → 0.103.15 (RUSTSEC-2026-0285, CVSS 5.3: TLS 1.3
  handshake messages accepted across encryption-level boundaries). Every
  rustls path in svc-jobs is a TLS client: `sqlx-core (tls-rustls)` to
  Postgres and `async-nats → rustls` to NATS.
- chacha20 0.10.2, spin 0.9.9 and event-listener 5.4.2 are already clean. The
  only advisory left in `cargo audit` is RUSTSEC-2023-0071 (rsa): it is listed
  in `Cargo.lock` only and not compiled. Its ignore in `deny.toml` now records
  the upstream status with the date it was checked (2026-09-28: no fixed rsa
  version), as the file's own policy requires.
- The lockstep publish republishes chart `br-svc-jobs` at 0.3.3 with unchanged
  templates and values. `charts/br-svc-jobs/Chart.yaml` is not edited:
  `scripts/publish.sh` stamps `version` and `appVersion` from the crate version
  when it packages the chart.

## 0.3.2 - 2026-09-08

Dependency-bump maintenance release. No wire, schema or behaviour change for
consumers.

- Rebuilt on `br-rust-common` v1.3.0 (from v1.2.0) and the `br-e2e-harness`
  test harness v1.2.0 (from v1.1.3). Both are additive minor releases; the
  service source is unchanged and every gate is green on the new pins.
- `bc-jobs` and `contract-jobs` move to 0.5.0 (minor, not patch): their public
  API surfaces `br-rust-common` types — the `bc-jobs` ports and domain
  references expose `br_core_events` types, and the `contract-jobs`
  `integration` feature returns `br_core_integration` coordinate types — so a
  rebuild on a new `br-rust-common` is a contract-relevant change for their
  consumers. The `contract-jobs` default (runner) footprint pulls no
  BotResources library and stays byte-identical.
- Helm chart realigned to 0.3.2 (`version` and `appVersion`) with the crate.

## 0.3.1 - 2026-09-01

- Terminal run facts are read through the closed failure-kind vocabulary of
  contract-jobs 0.4.0. A kind outside `TRANSIENT`/`PERMANENT` no longer reaches
  the domain at all; the settling path is unchanged, so such a fact is still
  acknowledged and discarded rather than redelivered forever.
- The status consumer dispatches on `RunnerStatusFact` from contract-jobs
  instead of its own string literals, and reads a status subject exactly once:
  the identity it resolved is what settles the frame and what names the metric,
  so the two can never disagree about which fact was dropped. One deliberate
  side effect: a frame naming no known fact is now acknowledged before its
  runner type is validated, so a subject invalid on both counts settles as an
  ack instead of a term — both settle the message definitively, nothing is
  redelivered either way.
- A runner fact this service could not read, or refused before it reached its
  job, now raises `jobs_runner_facts_discarded_total{fact}` on `/metrics`,
  alongside the warning it already logged. The two terminal series are
  registered at zero when the transport starts, so the alert is armed before the
  first drop. A fact the domain examined and declined is not counted — it was
  understood, not lost.

## 0.3.0 - 2026-08-27

- Added independent durable consumers for `job.cancel.v2`, `job.finish.v2`,
  and `job.fail.v2`; the version 1 durable names are deliberately unchanged so
  their JetStream cursors survive the upgrade.
- Version 2 commands enter the lifecycle-only domain path and retain the
  envelope actor as declarant attribution. Version 1 continues through the
  legacy owner-coherence path.
- Added a real Postgres/NATS oracle covering both versions, stable-command-id
  redelivery, lifecycle refusal after terminal resolution and soft deletion,
  exact failure cause and resolution identities, subscriptions, query state,
  affordances, and actor attribution.
- The v2 cancellation oracle exercises the complete tree semantics: descendant
  cascade, in-flight desired-state cancel entries, undelivered-trigger
  withdrawal, late runner completion, and one durable resolution per job.

## 0.2.0 - 2026-08-21

- Added the runner-type lifecycle migration and canonical Postgres adapter,
  including set-based decision-fact reads for snapshots and a locked retirement
  recheck that prevents an active-job race.
- Added ack-only administrator mutations for deprecation, reactivation, and
  retirement. Fleet reads and every typed fleet delta carry lifecycle and the
  complete current affordance set from the shared domain decisions.
- Added the Published Language adapter for `jobs.runner_type.*`; active and
  deprecated types are put, retired types retracted, and startup reconciles the
  full prefix. A post-commit KV failure is logged and healed later rather than
  turning a committed mutation into a false error verdict.
- Separated Job routing keys from presence-registered RunnerType aggregates;
  added per-pod periodic catalog reconciliation with readiness health —
  convergence is by reconciliation, with no cross-pod write ordering — and a
  durable, idempotent quiet-period affordance impact.

## 0.1.1 - 2026-08-19

- **The service survives a restart.** Boot no longer runs `ensure_app_role`
  unconditionally: a probe (`role_password_already_works`, the same guard
  svc-identity, svc-projects, svc-services, svc-tasks, svc-timesheet and
  svc-website carry) first asks whether `jobs_app` already accepts the
  configured password, and provisioning is skipped when it does. Under
  PostgreSQL 16/CNPG the unconditional `ALTER ROLE jobs_app PASSWORD …` was
  denied on every boot after the first (`permission denied to alter role`),
  crashlooping the pod. Non-credentials probe failures fail the boot loudly
  instead of falling through to the denied ALTER. Fixes contract breach
  `01a019f6-cdd1-783f-9aff-6700fcc02ed5` (registry patch 0.1.1).

## 0.1.0 - 2026-08-17

- The service: migrations realising the sealed 0.1 database schema (including
  the bounded `run_logs` range partitions and the three read views), the
  PostgreSQL adapters behind the `bc-jobs` ports, the GraphQL edge serving the
  sealed SDL (five queries, three ack-only mutations, four snapshot-then-delta
  subscriptions over SSE), the integration bus through
  `br-util-nats-fabric` (four durable command consumers, transactional outbox
  for the eight published events), the confined runner transport (trigger
  publication and withdrawal, status and log consumption, the desired-state
  cancel bucket, the presence watch), and the dispatch and backstop loops.
- A creation names its producing bounded context or it is refused: `producer` is
  required on `job.create`, so a payload omitting it does not deserialize and
  takes the malformed path — answered by a `creation_rejected` event carrying
  `malformed_payload` whenever the payload still names a job id. There is no
  fallback attribution: no source-derived key, no parent-derived key, no
  reserved key. A job is never recorded under a producer nobody declared, and a
  `source_bc` that disagrees with `producer` is refused.
- A presence entry announces `READY` or `DRAINING`. A `DRAINING` instance stays
  live and keeps the runs it already carries, but is excluded from its runner
  type's availability: while every live instance of a type drains, the type
  reports unavailable and dispatch waits instead of publishing a trigger nobody
  would take. An unknown status code does not parse, so the entry takes the
  unreadable path: it is logged, never read as `READY`, and it drains the live
  instance that wrote it.
- A presence entry also declares a `capacity` (required, at least 1): how many
  runs the instance carries at once. It is declarative — dispatch stays
  pull-based and capacity never gates delivery — and feeds the fleet reads: an
  instance is busy once its runs reach its declared capacity, and a runner type
  totals the capacity of its live, non-draining instances. A capacity that is
  absent, zero or negative never parses on the wire; one above the domain
  ceiling of 10 000 parses but is refused by the domain. Either way the entry
  is logged and refused, and the refusal drains the live instance that wrote it
  — the instance keeps its session and its runs, takes no new work, and the
  fleet keeps the last capacity it accepted.
- A presence entry an instance rewrote in an unreadable form is treated as
  `DRAINING` when that instance holds a live session: it keeps its session and
  its runs but takes no new work, instead of staying dispatchable for ever on
  the last readable report it managed to write. An unreadable entry naming no
  live instance is still ignored and logged.
- The backstop reconciles the presence bucket with the open sessions: it reads
  the bucket's keys once per sweep and closes, under `presence_expired`, every
  open session the bucket no longer backs, reclaiming its runs. It covers the
  two evictions no watch can see — one that lands while the pod is down, and
  one whose recording was abandoned under contention. The reconciliation is
  best-effort: a broker the sweep cannot reach is logged, and the backstops
  that need PostgreSQL alone still run.
- A reclaim takes the runs of the session that was lost, not every run the
  instance key ever carried: only a run started inside the closed session's
  window is failed for instance loss, so a run the replacement session is
  executing survives its predecessor's reclaim.
- The dispatch loop no longer spins when a runner type is fully draining: a
  pass that skipped a job because nobody would take it waits its full interval
  and is woken by the fleet fact of an instance reporting ready again, rather
  than re-reading a retry that is due but undispatchable every few hundred
  milliseconds. That long wait is taken only when nothing else in the pass asks
  for a sooner one — a dispatch that failed, or one that filled its batch,
  keeps the short wake, so a due retry on an available type is never delayed by
  an unrelated skip.
- Every knob is configuration, validated once at boot. The declared
  environment: `PORT`, `DATABASE_URL`, `DATABASE_URL_OWNER`, `NATS_URL`,
  `NATS_USER`, `NATS_PASSWORD`, `JOBS_APP_PASSWORD`; the domain and timing
  knobs `JOBS_INACTIVITY_TIMEOUT_SECONDS`, `JOBS_RUN_MAX_DURATION_SECONDS`,
  `JOBS_RETRY_BASE_DELAY_SECONDS`, `JOBS_RETRY_MAX_DELAY_SECONDS`,
  `JOBS_RETRY_FACTOR`, `JOBS_RETRY_JITTER_BASIS_POINTS`,
  `JOBS_MAX_ATTEMPTS_CEILING`, `JOBS_BACKSTOP_INTERVAL_SECONDS`,
  `JOBS_DISPATCH_MINIMUM_WAKE_MILLISECONDS`; the runner-transport consumer
  knobs `JOBS_CONSUMER_ACK_WAIT_SECONDS`, `JOBS_CONSUMER_MAX_ACK_PENDING`,
  `JOBS_CONSUMER_MAX_DELIVER`; and the background-task supervision knobs
  `JOBS_TASK_RESTART_INITIAL_BACKOFF_MILLISECONDS`,
  `JOBS_TASK_RESTART_MAX_BACKOFF_SECONDS`, `JOBS_TASK_RESTART_BUDGET`,
  `JOBS_TASK_STABILITY_SECONDS`; and the capacity knob
  `JOBS_LOG_PARTITION_HORIZON_WARNING_DAYS`.
- The runner-transport durable consumers declare their own `ack_wait`,
  `max_ack_pending` and `max_deliver` instead of inheriting the server
  defaults, so a handler slower than the server's grace period is never
  redelivered mid-processing.
- Concurrent writers are refused on the whole lifecycle state their decision
  read, not on a coarse summary of it: a job write re-reads the locked
  aggregate and compares every run's start, terminal, plan, steps, cancellation
  request and retry schedule, plus the resolution, the deletion and the manual
  retry; a fleet write locks its runner type and compares every live instance's
  session, version, reported status and change number. A duplicate or
  redelivered fact handled by two instances at once therefore writes one domain
  event and publishes one integration event, never two.
- Every background task is supervised: the death of any of them (error, silent
  return or panic) takes readiness DOWN with the task named as the reason and
  restarts it with bounded exponential backoff; readiness comes back UP only
  once every task runs again, and a task that exhausts its restart budget
  leaves the pod NOT READY for an operator. When the durable fact listener
  dies, every open subscription is terminated so clients reconnect onto a fresh
  snapshot on a healthy pod rather than freezing on a stream that stopped.
- A `CHECK` violation from PostgreSQL is treated as an infrastructure failure —
  redelivered and alerted — never as a domain refusal that discards a lifecycle
  fact.
- Settling a job writes the affordance changes it causes on its predecessor and
  on its descendants in the SAME transaction as the settling fact, on rows
  locked in one sorted pass with it. A crash can no longer land a terminal fact
  while losing the affordance refresh that fact caused, and no client is left
  with a stale action until it reconnects.
- Boot reports the `run_logs` partition horizon read from the live schema: the
  service says loudly how long log ingestion is covered, and errors once the
  remaining margin falls under `JOBS_LOG_PARTITION_HORIZON_WARNING_DAYS` or the
  horizon is exhausted. It never refuses to boot on it — the sealed spec ranks
  a lost log line below a lost lifecycle fact, so log capacity never takes the
  lifecycle path down.
- `jobsLogs` refuses `first` and `last` together with `first_and_last_together`
  instead of silently mixing one argument's direction with the other's limit.
- A subscriber that falls irrecoverably behind the in-process fact stream ends
  its subscription instead of skipping the facts it missed: the client
  reconnects onto a fresh snapshot, and no list silently keeps a stale row.
- Watching a job watches its whole tree: every fact carried by a descendant, and
  every descendant joining the tree, pushes the delta that carries the refreshed
  projection.
- The whole user-facing surface is platform-administrator only: a missing or
  undecodable passport is answered `UNAUTHENTICATED`, any other caller
  `FORBIDDEN`, both as structured GraphQL errors, before a query, a mutation or
  a subscription establishment reaches a resolver.
- `run_logs` is range-partitioned by month with no `DEFAULT` partition, and the
  migration declares partitions up to `2030-12-31`. This is deliberate — a
  missing partition fails loud rather than silently pooling rows — and it is an
  operational obligation: partitions covering the next months must be declared
  before the horizon is reached, or log ingestion stops and the log consumer
  redelivers indefinitely.
- Declared NATS streams and buckets are bound, never created: an absent stream
  or bucket fails the boot loudly — readiness goes down, the failure is logged
  and the process stops with a non-zero code rather than lingering as a pod that
  will never be ready.
