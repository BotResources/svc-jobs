# svc-jobs

The platform's shared execution ledger. Producers submit jobs for a runner
type; runners execute them; jobs records every attempt, exposes the live
runner fleet and publishes lifecycle events. It decides dispatch, retries and
cancellation transport — it never decides what the work means: the runner
configuration is forwarded opaque and unvalidated, results land in the owning
domain through that domain's own APIs, and only the *fact* of completion or
failure is recorded here.

Like the other BotResources generic services (`svc-auth`, `svc-notifier`),
svc-jobs ships as a portable container image
(`ghcr.io/botresources/br-svc-jobs`) plus a Helm chart, versioned per-repo
with a keepachangelog `CHANGELOG.md`, and built on the shared
[`br-rust-common`](https://github.com/BotResources/br-rust-common) library.

## Crates

| Crate | Role |
|---|---|
| [`contract-jobs`](crates/contract-jobs) | The published language: integration subjects, runner-transport wire shapes, stream/bucket names. The only crate consumers import — released by git tag `contract-jobs/v*`, semver-gated in CI. Two audiences: by default it carries the runner wire alone and pulls no BotResources library, so a runner binary stays free of `br-rust-common`; platform services publishing or consuming the integration subjects enable the `integration` feature for the typed `CommandCoords`/`EventCoords` constructors. |
| [`bc-jobs`](crates/bc-jobs) | The pure domain: the Job and RunnerType aggregates, commands, events, affordances, policies, ports. No I/O. |
| [`svc-jobs`](crates/svc-jobs) | The service binary: composition root, PostgreSQL adapters and migrations, GraphQL edge, integration bus, runner transport, dispatch and backstop loops. |

## How it works

- **Producers** create and control jobs through typed integration commands on
  the shared bus; every command is answered by a typed event (acceptance is
  `queued.v1`, refusal `creation_rejected.v1` — a producer never needs a
  timeout to learn its command's fate). Identical redeliveries are absorbed;
  conflicting reuse of an id is rejected.
- **Runners** never touch the GraphQL surface. All runner ↔ jobs interaction
  rides the NATS runner transport below, governed by NATS credentials.
- **State** is written once per fact: domain events land in `domain_events`
  with optimistic concurrency, integration events go out through a
  transactional outbox, and PostgreSQL `LISTEN/NOTIFY` fans committed facts out
  to every pod's live subscriptions. Multiple service instances are safe:
  concurrent writers re-read the locked aggregate and compare the whole
  lifecycle state, so a redelivered fact handled by two pods writes one domain
  event and publishes one integration event, never two.
- **Failure travels upward as information; cancellation travels downward as a
  command.** A child's failure never changes its parent — the owner receives
  the report and decides. Cancelling a job cancels every non-terminal
  descendant, withdraws undelivered triggers and writes desired-state cancel
  entries for runs in flight.
- **Retries**: after a transient run failure, jobs computes exponential
  backoff with jitter once and stores the due time; a runner `retry_after`
  hint may lengthen but never shorten it. A permanent failure ends automatic
  retry. Backstops reclaim abandoned work: a run exceeding the maximum
  duration fails as transient, and a job idle beyond the inactivity timeout
  fails with `INACTIVITY_TIMEOUT`.

## NATS surface

Everything below is **declared out of band and bound at boot, never created**:
an absent stream or bucket fails the boot loudly — readiness stays DOWN, the
failure is logged and the process exits non-zero. The service creates only its
own durable consumers.

### Integration bus (via `br-util-nats-fabric`)

Commands consumed on the `INTEGRATION_CMD` stream (durables
`svc_jobs_job_{create,cancel,finish,fail}`):

| Subject | Meaning |
|---|---|
| `integration.cmd.jobs.job.create.v1` | Create a job addressed to a runner type; the producer mints the UUIDv7 id and names its own bounded context in `producer` (required). |
| `integration.cmd.jobs.job.cancel.v1` | Cancel a job; cancellation propagates down the tree. |
| `integration.cmd.jobs.job.finish.v1` | The owner declares the job succeeded — the only path to `COMPLETED`. |
| `integration.cmd.jobs.job.fail.v1` | The owner declares the job failed (`DECLARED_BY_OWNER`). |

Events published on the `INTEGRATION_EVT` stream through the transactional
outbox: `integration.evt.jobs.job.{queued, creation_rejected, started,
plan_declared, step_started, completed, failed, cancelled}.v1` — every event
carries the caller-supplied job id.

A malformed command payload is never redelivered forever: an unreadable
creation that still names a job id is answered by a rejection event — a
creation naming no `producer` is exactly that case — a domain refusal is
acknowledged and discarded; only infrastructure failures and lost write races
are redelivered.

### Published runner-type catalog

Every active or deprecated runner type is published in the shared
`PUBLISHED_LANGUAGE` KV bucket under `jobs.runner_type.{runner_type}` as a
`contract_jobs::catalog::RunnerType`. The entry carries the type key, its
`ACTIVE` or `DEPRECATED` lifecycle and the wire `version` (v1, defaulting to 1
when absent). Retirement retracts the key;
reactivation recreates it. A lifecycle change reads the committed aggregate and
writes to the bucket after the transaction has closed — never with a
transaction open across the network call — and only a lifecycle fact projects:
presence traffic never reaches the bucket. Two pods writing the same key are
not ordered against each other: convergence is by reconciliation, and boot plus
the periodic reconciler repair the full prefix, which is also what repairs a
write a crash lost. An unavailable bucket cannot turn a committed mutation into
a false error; it removes the pod from readiness, and only a full
reconciliation puts it back.

**Who the owner is.** A resolution command is accepted only when its envelope
declares the same actor that declared the job (inherited along a manual-retry
chain). In practice the parent's runner resolves its child, the producing
bounded context resolves the root it declared, and platform administrators act
through the GraphQL surface. This is a coherence rule between self-declared
envelope identities, not an authentication: nothing on the bus verifies the
actor a publisher writes. Trust here is rooted in NATS access — first-party
publishers on a network-isolated segment — and impersonation on the bus is out
of scope by design.

### Runner transport

| Channel | Name / subject | Direction | Contract |
|---|---|---|---|
| Trigger stream | `JOBS_TRIGGER` / `jobs.trigger.{runner_type}` | jobs → runners | Persistent, acknowledged, at-least-once; instances of a type compete on a shared work-queue consumer. |
| Status stream | `JOBS_STATUS` / `jobs.status.{runner_type}.{started,plan_declared,step_started,completed,failed}` | runners → jobs | Persistent, acknowledged, at-least-once; carries start, plan, step and terminal run facts. |
| Log stream | `JOBS_LOG` / `jobs.log.{runner_type}` | runners → jobs | Size-bounded, discard-old; a lost log line is tolerated, a lost lifecycle fact is not — which is why logs never share the status stream. |
| Cancel bucket | `JOBS_CANCEL` KV, key `{run_id}` | jobs → runners | One desired-state entry per cancelled run, replayed on watch (re)connection; removed after the terminal fact, bucket TTL is the cleanup backstop. |
| Presence bucket | `JOBS_PRESENCE` KV, key `{runner_type}.{instance_key}` | runners → jobs | Expiring live state, heartbeat-rewritten; TTL eviction signals loss, graceful shutdown deletes immediately. The first entry of an unknown type registers the RunnerType. Watched with history, so establishing the watch replays the last entry per key before live updates. |

Runner-type and instance-key segments are validated by one shared alphabet
(`contract-jobs::segment::SubjectSegment`), so no input can widen a rendered
subject into a wildcard.

A presence entry declares two things about the instance that wrote it: a
`status` and a `capacity`.

`status` is a closed set of two codes:

| Code | Meaning |
|---|---|
| `READY` | Alive and taking new deliveries; counts towards the runner type's availability. |
| `DRAINING` | Alive, finishing the runs it already holds, taking no new deliveries; does **not** count towards availability. |

Draining is a status change, never a disconnection: the instance keeps its
presence session and its running runs are not reclaimed. When every live
instance of a type is `DRAINING`, the type reports `isAvailable: false` and
dispatch waits — no trigger is published that nobody would take.

`idleInstanceCount` counts the live instances with room left — fewer runs than
their declared capacity — draining ones included: idle is about load, not about
willingness to take more. Availability
is carried by `isAvailable` and by the room a type offers
(`totalCapacity`, which counts non-draining instances only), so a fleet that is
entirely draining reads as idle, unavailable, and offering no capacity.

`capacity` is the number of runs the instance carries at once — required, at
least 1, refreshed with every heartbeat and free to change over a session (a
rewrite with a different capacity is an ordinary presence change, exactly like a
status change). It is **declarative only**: dispatch stays pull-based and
capacity never gates trigger delivery. It feeds what the fleet reads: an
instance is busy once it carries as many runs as it declared
(`JobsRunnerInstance.capacity`), and a runner type totals the capacity of its
live, non-draining instances (`JobsRunnerType.totalCapacity`). A missing, zero
or negative capacity never parses on the wire; a capacity above 10 000 parses
but is refused by the domain. Either way the entry is refused, and an entry is
never read with a defaulted room.

**A presence entry this service refuses is never read as `READY`, and never
silently dropped.** Unreadable bytes, an unknown status code and a capacity
outside the accepted range all take the same path: an entry written by an
instance that already holds a live session records that instance as `DRAINING`
— it keeps its session and the runs it carries but takes no new work until an
entry the domain accepts says otherwise — and an entry naming no live instance
is ignored. Either way the refusal is logged with its code, and the fleet keeps
the last report it accepted: a refused number never reaches the projection.

## GraphQL surface — platform administrators only

The entire user-facing surface is restricted to platform administrators: a
missing or undecodable `X-Passport` is answered `UNAUTHENTICATED`, any other
caller `FORBIDDEN`, both as structured errors, before anything reaches a
resolver. No end-user, org-scoped or per-project access exists in this
version.

`X-Passport` is a trusted header: the service decodes it, it does not
authenticate its origin. Deploy svc-jobs only behind a gateway that strips
client-forged copies and re-injects the resolved passport, on a network
segment that blocks direct external access.

The name an administrator's action is recorded under (who cancelled, retried or
deleted) is read from the passport claim `display_name`; when that claim is
absent or blank, the administrator's user id is recorded instead. Jobs holds no
directory and resolves no name of its own, so the claim key is a per-project
seam: a deployment whose gateway names the claim differently records ids, not
names. This is a display convention, never an identity guarantee — the actor
recorded is always the passport's own id.

- **Queries** — `jobs` (filtered, paginated list), `jobsJob`,
  `jobsJobBySource` (the single non-terminal job for a source reference),
  `jobsLogs` (cursor-paginated, by job or run), `jobsFleet` (runner types and
  live instances).
- **Mutations — ack-only** (`{ success }`; state arrives via the streams) —
  `jobsCancelJob`, `jobsManualRetryJob` (creates a successor job from a
  pinned failed resolution, even past the retry budget), `jobsDeleteJob`
  (soft-delete of a terminal job; the audit trail remains), plus
  `jobsDeprecateRunnerType`, `jobsReactivateRunnerType` and
  `jobsRetireRunnerType`.
- **Subscriptions** (GraphQL over SSE: `POST /graphql` with
  `Accept: text/event-stream`) — `jobsChanged`, `jobsJobChanged` (the whole
  job tree), `jobsJobLogTail`, `jobsFleetChanged`. Every subscription first
  sends a snapshot equivalent to its initial read, then typed deltas;
  reconnecting starts with a fresh snapshot, so clients never need a separate
  refetch. Watching a job watches its whole tree.
- **Affordances** — every job snapshot and delta carries the backend-owned
  affordances (`cancel`, `manual_retry`, `delete` — allowed or
  blocked with a reason code); when affordances change without a job state
  change, a dedicated `JobsJobAffordancesChangedEvent` is emitted. Clients
  render these decisions, they never derive them.
  Every runner-type view and fleet delta likewise carries the complete
  `deprecate`, `reactivate`, and `retire` decisions. Retirement is available
  only for a deprecated type with no non-terminal job and no terminal run in
  the configured quiet period (default 24 h); reactivating a retired type
  requires live presence. If that boundary is the only changing fact, the
  durable timer emits a typed `RUNNER_TYPE_BECAME_RETIRABLE` delta at the
  boundary.

The full SDL is served at `GET /sdl` and printed by `svc-jobs schema` — one
document, two readers.

## Configuration

Everything is environment, validated once at boot: an invalid or inconsistent
combination (a default retry budget above its own ceiling, a max delay under
the base delay, an out-of-range jitter span…) refuses to boot rather than
failing job by job.

| Variable | Default | Meaning |
|---|---|---|
| `PORT` | `8006` | HTTP port (GraphQL, probes, metrics, SDL). |
| `DATABASE_URL` | — required | Runtime DSN (the `jobs_app` role). |
| `DATABASE_URL_OWNER` | falls back to `DATABASE_URL` | Owner DSN — migrations + grants at boot only; that pool then closes. |
| `JOBS_APP_PASSWORD` | unset | When set, the `jobs_app` role is created/updated at boot with this password and granted after migrating. |
| `TRUSTED_NETWORK_HOSTS` | unset | Per-host plaintext opt-out enforced by `br-util-postgres`: a plaintext DSN to a remote host boots only if that host is listed. |
| `NATS_URL` | — required | The NATS server carrying the integration bus and the runner transport. |
| `NATS_USER` / `NATS_PASSWORD` | unset | Optional credential pair — both or neither; one without the other refuses to boot. |
| `JOBS_INACTIVITY_TIMEOUT_SECONDS` | `86400` | An `IN_PROGRESS` job with no active run and no scheduled retry beyond this fails with `INACTIVITY_TIMEOUT`. |
| `JOBS_RUN_MAX_DURATION_SECONDS` | `259200` (72 h) | A `STARTED` run exceeding this fails as transient and gets a cancel entry. |
| `JOBS_RETRY_BASE_DELAY_SECONDS` | `10` | First automatic-retry delay. |
| `JOBS_RETRY_MAX_DELAY_SECONDS` | `3600` | Backoff ceiling. |
| `JOBS_RETRY_FACTOR` | `3` | Exponential backoff factor. |
| `JOBS_RETRY_JITTER_BASIS_POINTS` | `2000` | Jitter span applied to each computed delay. |
| `JOBS_MAX_ATTEMPTS_CEILING` | `10` | Service ceiling; a job's own `max_attempts` may lower, never exceed it (default per-job budget: 3). |
| `JOBS_BACKSTOP_INTERVAL_SECONDS` | `30` | Sweep interval for the run-duration, inactivity and cancel-entry backstops. |
| `JOBS_DISPATCH_MINIMUM_WAKE_MILLISECONDS` | `250` | Floor between dispatch passes. |
| `JOBS_RETIREMENT_QUIET_PERIOD_SECONDS` | `86400` (24 h) | A deprecated runner type may only be retired this long after its latest terminal run. |
| `JOBS_RUNNER_TYPE_IMPACT_INTERVAL_SECONDS` | `60` | Sweep interval of the durable timer that emits `RunnerTypeBecameRetirable` once the quiet period elapses. |
| `JOBS_CATALOG_RECONCILE_INTERVAL_SECONDS` | `30` | Interval of the reconciliation that repairs the whole published runner-type prefix and restores catalog readiness. |
| `JOBS_CONSUMER_ACK_WAIT_SECONDS` | `30` | Runner-transport consumer redelivery grace. |
| `JOBS_CONSUMER_MAX_ACK_PENDING` | `256` | Runner-transport consumer in-flight window. |
| `JOBS_CONSUMER_MAX_DELIVER` | `-1` (unlimited) | Runner-transport delivery budget; poison frames are terminated explicitly, never dropped on a budget. |
| `JOBS_TASK_RESTART_INITIAL_BACKOFF_MILLISECONDS` | `250` | Supervised-task restart backoff start. |
| `JOBS_TASK_RESTART_MAX_BACKOFF_SECONDS` | `30` | Supervised-task restart backoff ceiling. |
| `JOBS_TASK_RESTART_BUDGET` | `10` | Restarts before a task leaves the pod NOT READY for an operator. |
| `JOBS_TASK_STABILITY_SECONDS` | `60` | Uptime after which a task's restart budget resets. |
| `JOBS_LOG_PARTITION_HORIZON_WARNING_DAYS` | `180` | Margin below which boot logs the remaining `run_logs` partition horizon as an `error!`. |

## Probes & operations

| Endpoint | Meaning |
|---|---|
| `GET /livez` | Liveness. |
| `GET /readyz` | Readiness — DOWN while migrating, while any declared stream/bucket is unbound, until every supervised background task has bound its own source of work, and whenever one of them is dead; the reason names the culprit. |
| `GET /metrics` | Prometheus metrics. |
| `GET /sdl` | The GraphQL SDL the running binary serves. |
| `GET /graphql` | GraphiQL playground (passport-gated like the rest). |
| `svc-jobs schema` | Prints the same SDL and exits — for codegen and CI diffing, no infrastructure needed. |

Migrations are embedded in the binary and run at boot via the owner DSN.
Every background task (command consumers, status/log/presence consumers,
dispatch and backstop loops, the fact listener) is supervised: a death takes
readiness DOWN and restarts it with bounded backoff; exhausting the budget
leaves the pod NOT READY.

The known functional limitations (the lost-trigger gap, the narrow
presence-loss window, a job waiting forever on a runner type that never
appears — now with nothing in `jobsFleet` to show for it, since a type no
instance ever announced is not a registered aggregate — the partition horizon)
are recorded in [CHANGELOG.md](CHANGELOG.md) — the changelog is honest, read it
before integrating.

## Why it is the way it is

| Thing | Why |
|---|---|
| Streams, buckets and the database are bound, never created | Topology is declared out of band by the deployment; a service that auto-provisions infra hides drift. Absence fails the boot loudly instead of lingering as a pod that will never be ready. |
| The runner transport talks to NATS directly, confined to `src/runner_transport/` | The `jobs.*` streams and the cancel/presence buckets are this service's private wire with its runners, deliberately outside the platform's integration-bus conventions; no other component ever speaks it. That module is the only NATS seam besides the integration bus, and the boundary is deliberate: widening it is a design change, not a refactor. |
| `run_logs` has monthly range partitions (2026-01 → 2030-12) and **no `DEFAULT` partition** | A missing partition fails loud rather than silently pooling rows into an unbounded default. Operational obligation: declare the next partitions before the horizon, or log ingestion stops (lifecycle facts are unaffected). |
| `JOBS_CANCEL` bucket TTL must track `JOBS_RUN_MAX_DURATION_SECONDS` | The bucket's expiry is the cleanup backstop for cancel entries whose terminal fact never arrived; a TTL shorter than the maximum run duration can drop a stop request a slow run still needs. Keep the two aligned in the topology declaration. |
| Logs ride their own size-bounded, discard-old stream | A log flood can neither delay a status fact nor evict one; losing an old log line is acceptable, losing a lifecycle fact is not. |
| A reclaim only takes runs started inside the closed presence session's window | An instance key outlives its sessions: reclaiming by key alone would fail a run the replacement session is executing. The session that carried the run owns the reclaim. |
| The service clock truncates every instant it mints to microseconds | `timestamptz` keeps microseconds, a Linux clock reads nanoseconds. An instant minted at nanosecond precision is carried one way by the event a subscriber folds and another way by the column a later read returns — the same moment, two values. Truncating where the instant is minted is the only place that keeps the two identical; every timestamp the service authors comes from that clock, never from an ambient `Utc::now()`. |
| A supervised task is down until it says it is bound, not from the moment it is spawned | A subscription is lossy exactly while it is being established: a KV put or a `NOTIFY` landing in that window is missed forever, and nothing reconciles that direction. Counting a task as up on spawn let the pod accept traffic — and a client accept a snapshot — before the deltas behind it could arrive. The task itself is the only place that knows when its source of work is bound, so the task signals it. |
| The presence watch replays the last entry per key instead of taking live updates only | Reconciliation only closes a session whose KV entry vanished, never the reverse, so a put missed during establishment stays invisible until the runner's next heartbeat. A replay costs nothing — an entry for a session already known produces no event. |
| Boot probes whether `jobs_app` already accepts its password before provisioning it | `ensure_app_role` guards its CREATE but runs `ALTER ROLE … PASSWORD` unconditionally, and PostgreSQL 16 denies that ALTER from the second boot on: CNPG's roles reconciler revokes the membership `jobs_owner` gained by creating `jobs_app`, and CREATEROLE alone confers no authority over a role held without ADMIN OPTION. Asking the catalog first is what keeps the pod out of CrashLoopBackOff (see 0.1.1 in the changelog). The probe treats only SQLSTATE 28P01/28000/3D000 as "not provisioned yet" — anything else surfaces loud rather than falling through to the denied ALTER — and it covers restart, not rotation: once the password changes, rotating it belongs to the CNPG roles reconciler that owns the role, never to a booting service. |
| Mutations return `{ success }` only | State arrives through the subscriptions' snapshot-then-delta stream; a mutation that returned a DTO would race its own event. |
| `runner_types` is a routing dictionary, `registered_runner_types` is the aggregate | A key referenced by a job is only a route; the business registration happens when presence is observed. Splitting the two is what lets a job name a type that has never run without inventing a lifecycle for it. |
| The retirement quiet period is stored as the terminal instant, never as a materialized `eligible_at` | The period is a deployment knob; a row that baked the old value in would keep answering with it after the knob moved. The sweeper ages the stored terminal instant against the configured period at read time. |
| A run terminal takes no runner-type lock | Retirement re-reads its facts under that lock, and it also refuses any type with a non-terminal job — a run whose terminal is committing belongs to a job that is unresolved in the same transaction, so the count clause already covers the race. Locking every terminal would serialize all run completions of a type for a decision taken a few times in a service's life. |
| The durable fact listener restarts instead of reconnecting silently | `PgListener::recv` reconnects on its own and drops every `NOTIFY` raised in the gap, leaving subscribers on a live but lossy stream. `try_recv` surfaces the loss, the supervisor restarts the task, its teardown cuts every subscription, and the clients come back onto fresh snapshots. |
| The Published Language catalog write happens after the transaction commits, and nothing orders two pods writing the same key | A KV round-trip inside a transaction holds its snapshot and locks for the whole network call, and a lock taken to order the write would die at commit — before the write it was meant to order. Nothing in the bucket API compares revisions, so the honest guarantee is convergence by reconciliation: a raced or lost write is repaired by the periodic reconciler, and readiness only returns after a full reconciliation. |
| `scripts/setup-branch-protection.sh` + `.github/required-checks.json` | Declarative source of truth for required checks; each entry must match a `ci.yml` job `name:` verbatim or PRs block forever waiting for a check that never reports. |
| Changelog headings are plain `## x.y.z` | The release pipeline greps that exact form (root for the image, per-crate for tags); a bracketed keepachangelog heading ships nothing. |

## Versioning & release

- The service version lives in `crates/svc-jobs/Cargo.toml`; the Helm chart's
  `version`/`appVersion` are kept in lockstep. CD is image-first, tag-after: on
  a version bump landing on `main` with a matching `## x.y.z` heading in
  `CHANGELOG.md`, CI-validated code is built (static-musl via cargo-zigbuild,
  linux/amd64 + linux/arm64), pushed to
  `ghcr.io/botresources/br-svc-jobs:{version}` with the chart at
  `oci://ghcr.io/botresources/charts/br-svc-jobs`, and only then tagged
  `svc-jobs/v{version}` — a tag is a receipt, not a trigger.
- `contract-jobs` releases as a plain git tag (`contract-jobs/v{version}`) when
  its version bumps — consumers pull it as a git dependency. CI gates it with
  `cargo-semver-checks` against its latest release tag.
- `scripts/publish.sh` drives the build (`--dry-run`, `--local-image`,
  `--check-only` for local use; CD runs `--skip-checks` after CI has passed).

## License

Apache-2.0 — see [LICENSE](LICENSE). This repository is published read-only and
does not accept external contributions; see
[CONTRIBUTING.md](CONTRIBUTING.md) and [SUPPORT.md](SUPPORT.md).
