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
| [`contract-jobs`](crates/contract-jobs) | The published language: integration subjects, runner-transport wire shapes, stream/bucket names. The only crate consumers import — released by git tag `contract-jobs/v*`, semver-gated in CI. |
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
| `integration.cmd.jobs.job.create.v1` | Create a job addressed to a runner type; the producer mints the UUIDv7 id. |
| `integration.cmd.jobs.job.cancel.v1` | Cancel a job; cancellation propagates down the tree. |
| `integration.cmd.jobs.job.finish.v1` | The owner declares the job succeeded — the only path to `COMPLETED`. |
| `integration.cmd.jobs.job.fail.v1` | The owner declares the job failed (`DECLARED_BY_OWNER`). |

Events published on the `INTEGRATION_EVT` stream through the transactional
outbox: `integration.evt.jobs.job.{queued, creation_rejected, started,
plan_declared, step_started, completed, failed, cancelled}.v1` — every event
carries the caller-supplied job id.

A malformed command payload is never redelivered forever: an unreadable
creation that still names a job id is answered by a rejection event; a domain
refusal is acknowledged and discarded; only infrastructure failures and lost
write races are redelivered.

### Runner transport

| Channel | Name / subject | Direction | Contract |
|---|---|---|---|
| Trigger stream | `JOBS_TRIGGER` / `jobs.trigger.{runner_type}` | jobs → runners | Persistent, acknowledged, at-least-once; instances of a type compete on a shared work-queue consumer. |
| Status stream | `JOBS_STATUS` / `jobs.status.{runner_type}.{started,plan_declared,step_started,completed,failed}` | runners → jobs | Persistent, acknowledged, at-least-once; carries start, plan, step and terminal run facts. |
| Log stream | `JOBS_LOG` / `jobs.log.{runner_type}` | runners → jobs | Size-bounded, discard-old; a lost log line is tolerated, a lost lifecycle fact is not — which is why logs never share the status stream. |
| Cancel bucket | `JOBS_CANCEL` KV, key `{run_id}` | jobs → runners | One desired-state entry per cancelled run, replayed on watch (re)connection; removed after the terminal fact, bucket TTL is the cleanup backstop. |
| Presence bucket | `JOBS_PRESENCE` KV, key `{runner_type}.{instance_key}` | runners → jobs | Expiring live state, heartbeat-rewritten; TTL eviction signals loss, graceful shutdown deletes immediately. The first entry of an unknown type registers the RunnerType. |

Runner-type and instance-key segments are validated by one shared alphabet
(`contract-jobs::segment::SubjectSegment`), so no input can widen a rendered
subject into a wildcard.

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

- **Queries** — `jobs` (filtered, paginated list), `jobsJob`,
  `jobsJobBySource` (the single non-terminal job for a source reference),
  `jobsLogs` (cursor-paginated, by job or run), `jobsFleet` (runner types and
  live instances).
- **Mutations — ack-only** (`{ success }`; state arrives via the streams) —
  `jobsCancelJob`, `jobsManualRetryJob` (creates a successor job from a
  pinned failed resolution, even past the retry budget), `jobsDeleteJob`
  (soft-delete of a terminal job; the audit trail remains).
- **Subscriptions** (GraphQL over SSE: `POST /graphql` with
  `Accept: text/event-stream`) — `jobsChanged`, `jobsJobChanged` (the whole
  job tree), `jobsJobLogTail`, `jobsFleetChanged`. Every subscription first
  sends a snapshot equivalent to its initial read, then typed deltas;
  reconnecting starts with a fresh snapshot, so clients never need a separate
  refetch. Watching a job watches its whole tree.
- **Affordances** — every job snapshot and delta carries the backend-owned
  affordances (`CANCEL_JOB`, `MANUAL_RETRY_JOB`, `DELETE_JOB` — allowed or
  blocked with a reason code); when affordances change without a job state
  change, a dedicated `JobsJobAffordancesChangedEvent` is emitted. Clients
  render these decisions, they never derive them.

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
| `JOBS_CONSUMER_ACK_WAIT_SECONDS` | `30` | Runner-transport consumer redelivery grace. |
| `JOBS_CONSUMER_MAX_ACK_PENDING` | `256` | Runner-transport consumer in-flight window. |
| `JOBS_CONSUMER_MAX_DELIVER` | `-1` (unlimited) | Runner-transport delivery budget; poison frames are terminated explicitly, never dropped on a budget. |
| `JOBS_TASK_RESTART_INITIAL_BACKOFF_MILLISECONDS` | `250` | Supervised-task restart backoff start. |
| `JOBS_TASK_RESTART_MAX_BACKOFF_SECONDS` | `30` | Supervised-task restart backoff ceiling. |
| `JOBS_TASK_RESTART_BUDGET` | `10` | Restarts before a task leaves the pod NOT READY for an operator. |
| `JOBS_TASK_STABILITY_SECONDS` | `60` | Uptime after which a task's restart budget resets. |

## Probes & operations

| Endpoint | Meaning |
|---|---|
| `GET /livez` | Liveness. |
| `GET /readyz` | Readiness — DOWN while migrating, while any declared stream/bucket is unbound, and whenever a supervised background task is dead; the reason names the culprit. |
| `GET /metrics` | Prometheus metrics. |
| `GET /sdl` | The GraphQL SDL the running binary serves. |
| `GET /graphql` | GraphiQL playground (passport-gated like the rest). |
| `svc-jobs schema` | Prints the same SDL and exits — for codegen and CI diffing, no infrastructure needed. |

Migrations are embedded in the binary and run at boot via the owner DSN.
Every background task (command consumers, status/log/presence consumers,
dispatch and backstop loops, the fact listener) is supervised: a death takes
readiness DOWN and restarts it with bounded backoff; exhausting the budget
leaves the pod NOT READY.

The known functional limitations of 0.1 (owner authorization enforced as
declaring-actor, the lost-trigger gap, the partition horizon) are recorded in
[CHANGELOG.md](CHANGELOG.md) — the changelog is honest, read it before
integrating.

## Why it is the way it is

| Thing | Why |
|---|---|
| Streams, buckets and the database are bound, never created | Topology is declared out of band by the deployment; a service that auto-provisions infra hides drift. Absence fails the boot loudly instead of lingering as a pod that will never be ready. |
| `run_logs` has monthly range partitions (2026-01 → 2030-12) and **no `DEFAULT` partition** | A missing partition fails loud rather than silently pooling rows into an unbounded default. Operational obligation: declare the next partitions before the horizon, or log ingestion stops (lifecycle facts are unaffected). |
| `JOBS_CANCEL` bucket TTL must track `JOBS_RUN_MAX_DURATION_SECONDS` | The bucket's expiry is the cleanup backstop for cancel entries whose terminal fact never arrived; a TTL shorter than the maximum run duration can drop a stop request a slow run still needs. Keep the two aligned in the topology declaration. |
| Logs ride their own size-bounded, discard-old stream | A log flood can neither delay a status fact nor evict one; losing an old log line is acceptable, losing a lifecycle fact is not. |
| Mutations return `{ success }` only | State arrives through the subscriptions' snapshot-then-delta stream; a mutation that returned a DTO would race its own event. |
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
