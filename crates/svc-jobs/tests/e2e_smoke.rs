//! The one scenario `svc-jobs` ships with: it publishes the schema the
//! platform composes, and it serves the probes the platform operates it through.
//!
//! A skeleton has no domain to assert yet, and this is deliberately not filler.
//! These are the surfaces whose failure is **silent**: a broken `/readyz` takes
//! the pod out of the load balancer for good, a broken `/livez` has the kubelet
//! restart a healthy process forever, and a `/sdl` that disagrees with the
//! printed schema ships a supergraph no running service answers. None of them
//! turns anything red on its own. This file is what turns them red — and it is
//! what makes the `e2e-jobs` CI job a gate instead of a green square from
//! the day the service was scaffolded.
//!
//! It reads in two movements — the schema as CD reads it, then the running
//! service as the platform operates it — but **nothing here is conditional**.
//!
//! There is no skip, and that is a decision rather than an omission. An
//! end-to-end test's entire claim is that it exercised the real thing; a run
//! that could not reach a database has not made that claim, and reporting
//! success anyway is the same worthless green this file exists to eliminate —
//! only wearing a sympathetic costume. So a missing database is a **failure**,
//! on a CI runner and on a laptop alike, and the failure message is the product:
//! it names what is missing and exactly how to provide it.
//!
//! Real infrastructure, no mocks. A mocked pool proves the mock migrates and a
//! mocked probe proves nothing at all. The database is minted for this run and
//! dropped after it, so scenarios never inherit each other's state.
//!
//! **This file is the reference to extend.** A real scenario differs from the
//! second movement by what it does between readiness and teardown, never by how
//! it gets there: same fixture, same spawn, same poll-against-a-deadline, same
//! teardown. Add scenarios as sibling files in `tests/`; leave this one asserting
//! the contract every service owes the platform.

use std::process::Command;
use std::time::Duration;

use async_graphql::parser::parse_schema;
use async_graphql::parser::types::{ServiceDocument, TypeKind, TypeSystemDefinition};
use br_test_harness::{E2eDatabase, SpawnedNats, SpawnedProcess};
use reqwest::StatusCode;

/// The binary under test.
///
/// Cargo hands over the path of this crate's own `svc-jobs` bin target, so
/// every assertion below is made against the artifact CD publishes rather than
/// against a library relinked with test-only cfgs. It is also why the bin target
/// must keep its name: rename it and this constant stops existing.
const BIN: &str = env!("CARGO_BIN_EXE_svc-jobs");

/// The prefix doctrine puts on every root field of this subgraph.
///
/// The gateway flattens all subgraphs into one namespace, so an unprefixed
/// `health` would collide with every other service's — and resolve to whichever
/// subgraph the composer happened to read first.
const ROOT_FIELD_PREFIX: &str = "jobs";

/// The least-privilege runtime role the service serves under — the same name
/// GitOps declares on the CNPG cluster, so the fixture and production disagree
/// about nothing but the password.
const APP_ROLE: &str = "jobs_app";

/// How long the boot may take before the scenario calls it a failure.
///
/// Generous on purpose: the boot runs migrations against a database created
/// moments earlier, on a CI runner that is also compiling. The deadline bounds a
/// hang, it does not measure performance — tightening it buys nothing and costs
/// a flake.
const BOOT_TIMEOUT: Duration = Duration::from_secs(60);

#[tokio::test]
async fn the_scaffolded_service_serves_its_schema_and_every_operational_probe() {
    // ── The schema, as CD reads it ───────────────────────────────────────────
    //
    // First because it is the cheaper failure to diagnose: a broken schema
    // reported as a broken schema is worth more than the same break surfacing
    // later, behind whatever the database did.
    let printed = schema_subcommand_stdout();
    assert_no_log_line_precedes_the_document(&printed);
    let document = parse_or_panic(&printed);
    assert_every_root_field_carries_the_bounded_context_prefix(&document);
    assert_the_placeholder_query_is_the_only_query(&document);

    // ── The running service, as the platform operates it ─────────────────────
    require_provisioned_infrastructure();
    boot_and_assert_every_probe(&printed).await;
}

// ── The schema ──────────────────────────────────────────────────────────────

/// Run `svc-jobs schema` and return its stdout verbatim, newline included.
///
/// Spawned with every infrastructure variable REMOVED, on purpose. Printing the
/// schema is the one thing this binary must be able to do with nothing in reach:
/// CD runs it against the freshly built image, in a step that has no cluster, to
/// obtain the document it poses on the registry PatchVersion. A `schema` that
/// quietly needed a `DATABASE_URL` would work on every developer machine and
/// fail exactly once — at release time, on the release itself.
fn schema_subcommand_stdout() -> String {
    let output = Command::new(BIN)
        .arg("schema")
        .env_remove("DATABASE_URL")
        .env_remove("DATABASE_URL_OWNER")
        .env_remove("NATS_URL")
        .env_remove("JOBS_APP_PASSWORD")
        .output()
        .expect("`svc-jobs schema` should be spawnable — was the bin target built?");

    assert!(
        output.status.success(),
        "`svc-jobs schema` must exit 0 with no database and no bus in reach, got {:?}\n\
         stderr:\n{}",
        output.status.code(),
        String::from_utf8_lossy(&output.stderr),
    );

    let stdout = String::from_utf8(output.stdout)
        .expect("the SDL must be valid UTF-8 — the registry stores it as text");

    assert!(
        !stdout.trim().is_empty(),
        "`svc-jobs schema` printed nothing, so CD would pose an empty document on the \
         registry PatchVersion and the gateway would compose a supergraph without this subgraph",
    );

    stdout
}

/// The load-bearing assertion of the schema half, and the reason it is checked
/// before the document is even parsed.
///
/// `init_logging` installs a JSON subscriber that writes one object per line to
/// STDOUT, and CD parses this command's whole stdout as the SDL — so a single log
/// line emitted ahead of the document corrupts the release, silently, on the
/// release itself. The `schema` branch in `main.rs` returns before logging is
/// ever initialised; this asserts it stays that way, and it comes first because
/// the alternative diagnosis is a parser error pointing at column 1 of a JSON
/// record, which names the symptom instead of the cause.
fn assert_no_log_line_precedes_the_document(printed: &str) {
    let first = printed
        .lines()
        .find(|line| !line.trim().is_empty())
        .expect("a non-empty document has a first non-empty line");

    assert!(
        !first.trim_start().starts_with('{'),
        "the first line of `svc-jobs schema` looks like a JSON log record, not SDL — \
         something logs before the `schema` branch in main.rs prints the document: {first}",
    );
}

fn parse_or_panic(printed: &str) -> ServiceDocument {
    parse_schema(printed).unwrap_or_else(|error| {
        panic!(
            "`svc-jobs schema` must print a document the registry and the gateway can \
             parse: {error}\n---\n{printed}\n---"
        )
    })
}

/// Principle 25, on the published names.
///
/// All three roots are walked even though a skeleton only has a query: the
/// prefix rule applies to mutations and subscriptions too, and a rule checked
/// only where it currently has subjects is a rule broken by whoever adds the
/// first one.
fn assert_every_root_field_carries_the_bounded_context_prefix(document: &ServiceDocument) {
    let roots = root_types(document);

    assert!(
        !fields_of(document, &roots.query).is_empty(),
        "the subgraph must expose at least one query — a schema with no root field is not a valid \
         GraphQL document and the gateway cannot compose it",
    );

    for root in [&roots.query, &roots.mutation, &roots.subscription] {
        for field in fields_of(document, root) {
            assert!(
                field.starts_with(ROOT_FIELD_PREFIX),
                "root field `{root}.{field}` is not prefixed with `{ROOT_FIELD_PREFIX}` — the \
                 gateway flattens every subgraph into one namespace, so an unprefixed root field \
                 collides with another service's",
            );
        }
    }
}

/// A tripwire, not a rule: delete it the moment a real query exists.
///
/// It is here so that on the day this service was scaffolded its e2e job already
/// had something true to say — a job whose test binary contains no assertion
/// reports exactly the same green as one that proved the service works.
fn assert_the_placeholder_query_is_the_only_query(document: &ServiceDocument) {
    assert_eq!(
        fields_of(document, &root_types(document).query),
        vec![format!("{ROOT_FIELD_PREFIX}Health")],
        "the skeleton's placeholder query is gone or has company — if this service now has real \
         queries, delete this assertion and let the parse and prefix ones above carry the schema \
         contract",
    );
}

/// The type names this schema uses for its three roots.
struct RootTypes {
    query: String,
    mutation: String,
    subscription: String,
}

/// Resolve the root type names from the document's own `schema { … }` block
/// rather than assuming `Query` / `Mutation` / `Subscription`.
///
/// `async-graphql` names each root after the Rust type that implements it, so
/// this subgraph publishes `schema { query: QueryRoot }` and there is no type
/// called `Query` in it at all. A check hard-coded to the conventional names
/// finds nothing, iterates an empty list and passes — the worst possible outcome
/// for an assertion whose whole job is to catch an unprefixed field.
///
/// The conventional names are the fallback because the `schema` block is
/// optional in SDL exactly when the roots already carry them.
fn root_types(document: &ServiceDocument) -> RootTypes {
    let declared = document
        .definitions
        .iter()
        .find_map(|definition| match definition {
            TypeSystemDefinition::Schema(schema) => Some(&schema.node),
            _ => None,
        });

    RootTypes {
        query: declared
            .and_then(|schema| schema.query.as_ref())
            .map_or_else(|| String::from("Query"), |name| name.node.to_string()),
        mutation: declared
            .and_then(|schema| schema.mutation.as_ref())
            .map_or_else(|| String::from("Mutation"), |name| name.node.to_string()),
        subscription: declared
            .and_then(|schema| schema.subscription.as_ref())
            .map_or_else(
                || String::from("Subscription"),
                |name| name.node.to_string(),
            ),
    }
}

/// The field names declared on one object type, in schema order. Empty when the
/// type is absent, which is the honest answer for a root the schema omits.
///
/// Reads the parsed document rather than grepping the text: `contains("jobs")`
/// would happily match a description, an argument name or a type name, and a
/// prefix rule proved by a substring is a prefix rule that is not proved.
fn fields_of(document: &ServiceDocument, type_name: &str) -> Vec<String> {
    document
        .definitions
        .iter()
        .filter_map(|definition| match definition {
            TypeSystemDefinition::Type(declared) if declared.node.name.node == type_name => {
                match &declared.node.kind {
                    TypeKind::Object(object) => Some(object),
                    _ => None,
                }
            }
            _ => None,
        })
        .flat_map(|object| {
            object
                .fields
                .iter()
                .map(|field| field.node.name.node.to_string())
        })
        .collect()
}

// ── The running service ─────────────────────────────────────────────────────

/// Fail, with the remedy, when there is no database to run against.
///
/// Not a skip. This scenario boots the real binary against a real PostgreSQL,
/// and a run that could not do so has proved nothing — so reporting success
/// would be reporting a green gate over a service nobody booted, which is the
/// precise failure this file was written to eliminate. It is worth no less on a
/// laptop than in CI: a developer who is told "ok" has been told the service
/// works.
///
/// The message is therefore the product. Someone hitting this must be able to
/// act on it without opening this file, so it says what is missing, why the
/// scenario cannot proceed without it, and both ways to provide it.
///
/// It covers the *missing* variable only. A variable that is set but points at
/// nothing already fails inside the harness, naming the URL and the connection
/// error — a pre-flight connection here would add a dependency in order to say
/// less than that.
fn require_provisioned_infrastructure() {
    assert!(
        std::env::var("E2E_PG_ADMIN_URL").is_ok(),
        "\n\
         E2E_PG_ADMIN_URL is unset, so this scenario cannot run.\n\
         \n\
         It is an end-to-end test: it boots the real svc-jobs binary against a real\n\
         PostgreSQL database it mints for the run, and there is no version of it that proves\n\
         anything without one. A missing database is a failure here, never a skip.\n\
         \n\
         Provide one, then re-run:\n\n    \
         bash scripts/provision-e2e-infra.sh\n\
         \n\
         or point E2E_PG_ADMIN_URL at a PostgreSQL whose role can CREATE ROLE and CREATE\n\
         DATABASE, for instance:\n\n    \
         E2E_PG_ADMIN_URL=postgres://postgres@localhost:5432/postgres\n",
    );
}

/// Boot the real binary against real infrastructure and assert every probe,
/// including that `/sdl` serves exactly what the `schema` subcommand printed.
async fn boot_and_assert_every_probe(printed: &str) {
    // The migration role, minted with the posture GitOps declares for it:
    // BYPASSRLS, because a data migration running under FORCE RLS silently
    // no-ops instead of failing (doctrine #30). It runs the DDL and nothing
    // else — the binary closes its pool before the first request is served.
    let db = E2eDatabase::create(true, &[]).await;

    // The runtime role's password is alphanumeric by construction, and that is
    // not incidental: the deployment builds `DATABASE_URL` by interpolating the
    // password into a URL, so a `/` or a `+` in it corrupts the DSN and the
    // driver authenticates with the wrong secret (doctrine #32). The harness's
    // per-run database name is already a bare hex UUID, so it is a sound source.
    let app_password = db.db_name().replace('_', "");
    let db = db.with_app_role(APP_ROLE, &app_password).await;

    // `JOBS_APP_PASSWORD` is deliberately absent from the environment
    // below. With it set the binary provisions `jobs_app` itself; here the
    // harness has already created that role and given it the owner's default
    // privileges, so the service serves under the RLS-subject runtime role
    // either way — which is what this scenario is about — without depending on a
    // role grant it does not assert. Set it the day a scenario asserts the
    // provisioning itself.
    let owner_url = db.owner_migration_url();
    let app_url = db.app_url();

    // Drawn from below the kernel's ephemeral range, so a server that binds port
    // 0 elsewhere in this process can never be handed the same number.
    let port = free_port();
    let port_text = port.to_string();
    let base_url = format!("http://127.0.0.1:{port}");

    // The registry declares this major as one that speaks on the bus, so the
    // composition root connects to NATS before it reports ready — a service that
    // cannot reach the fabric fails its boot rather than serving queries while
    // dropping every integration message. Its own ephemeral server, never a
    // shared one: a scenario that observes another scenario's subjects passes
    // for the wrong reason.
    //
    // The harness spawns `nats-server` from PATH and panics, naming it, when it
    // is not there — `scripts/provision-e2e-infra.sh` installs it in CI, and
    // `brew install nats-server` covers a laptop.
    let nats = SpawnedNats::start().await;
    let nats_url = nats.url();

    let env: Vec<(&str, &str)> = vec![
        // The runtime pool: the RLS-subject role, exactly as in production.
        ("DATABASE_URL", app_url.as_str()),
        // The migration pool: owner-only, opened and closed inside the boot.
        ("DATABASE_URL_OWNER", owner_url.as_str()),
        // Loopback only. A test binary has no business listening on every
        // interface of whatever machine happens to be running it.
        ("HOST", "127.0.0.1"),
        ("PORT", port_text.as_str()),
        ("ENVIRONMENT", "local"),
        ("RUST_LOG", "info"),
        ("NATS_URL", nats_url.as_str()),
    ];

    let mut service = SpawnedProcess::spawn(BIN, &[], &env);

    // `/readyz` starts DOWN by design and flips only once the migrations, the
    // pool and every other declared dependency are up — so the only correct wait
    // is a poll against a deadline. A fixed sleep passes on a fast machine and
    // flakes on a loaded runner, and a suite that flakes is a suite nobody reads
    // the output of.
    let outcome = service
        .await_boot(&format!("{base_url}/readyz"), BOOT_TIMEOUT)
        .await;
    assert!(
        outcome.is_ready(),
        "svc-jobs never reported ready within {BOOT_TIMEOUT:?} ({outcome:?})\nlogs:\n{}",
        service.logs(),
    );

    let http = reqwest::Client::new();

    // Liveness answers a different question from readiness — "is this process
    // wedged", not "should it get traffic" — and the kubelet acts on it by
    // killing the pod. It is asserted separately for that reason: a `/livez`
    // accidentally wired to the readiness gate would restart a service that is
    // merely waiting on a dependency, turning a delay into a crash loop.
    let (status, _) = get(&http, &base_url, "/livez").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "/livez must be 200 on a running process — the kubelet restarts the pod on anything else",
    );

    let (status, body) = get(&http, &base_url, "/metrics").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "/metrics must be 200 — Prometheus scrapes it and a non-200 is a service with no telemetry",
    );
    assert!(
        body.contains("http_requests_total"),
        "/metrics must render the HTTP series the observability layer describes at boot; a 200 \
         without them means the recorder was never installed:\n{body}",
    );

    // The assertion the boot half exists for. The gateway's composer reads
    // `/sdl` from the running pod, while CD reads the `schema` subcommand of the
    // image it is releasing. When those two disagree, the supergraph describes a
    // schema no running service answers — and every symptom appears at the edge,
    // far from the cause.
    let (status, served_sdl) = get(&http, &base_url, "/sdl").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "/sdl must be 200 — the gateway's composer cannot build a supergraph without it",
    );

    // The one permitted difference is the line terminator `println!` appends —
    // exactly one, stripped explicitly rather than trimmed, so a document that
    // genuinely ends in a blank line is still compared honestly.
    let printed = printed.strip_suffix('\n').unwrap_or(printed);
    assert_eq!(
        served_sdl, printed,
        "the schema served at /sdl and the schema printed by `svc-jobs schema` must be the \
         same document — the composer reads the first and CD poses the second",
    );

    service.shutdown().await;
    nats.shutdown().await;
    db.cleanup().await;
}

/// GET a path off the running service and return its status with its body.
///
/// Both, always: a status assertion whose failure message cannot show the body
/// sends the reader back to re-run the suite by hand to learn what happened.
async fn get(http: &reqwest::Client, base_url: &str, path: &str) -> (StatusCode, String) {
    let response = http
        .get(format!("{base_url}{path}"))
        .send()
        .await
        .unwrap_or_else(|error| panic!("GET {path} against the running service failed: {error}"));

    let status = response.status();
    let body = response
        .text()
        .await
        .unwrap_or_else(|error| panic!("reading the body of GET {path} failed: {error}"));

    (status, body)
}

// ── Port allocation ─────────────────────────────────────────────────────────

/// A port that was free when probed, drawn from below the kernel's ephemeral
/// range so an auto-assigned socket (`nats-server` binds port 0) can never be
/// handed the same number. The upstream `br-test-harness` exports no
/// allocator, so the suite carries its own; move it to a shared module when a
/// second scenario file arrives.
fn free_port() -> u16 {
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicU16, Ordering};

    // Below the lowest ephemeral floor of the platforms this suite runs on
    // (Linux 32768, macOS 49152), above the IANA registered-port boundary, and
    // clear of the service defaults the charts use (8080, 4222, 5432).
    const RANGE_START: u16 = 20_000;
    const RANGE_END: u16 = 32_000;
    // Exhausting this means 64 consecutive ports are bound, which is a leaked
    // child, not contention — and the panic says so.
    const MAX_PROBES: u16 = 64;

    // Seeded from the PID so two test binaries running side by side do not both
    // start at RANGE_START and collide on every allocation. A racing double
    // seed is harmless: both writers store the same value.
    static NEXT: AtomicU16 = AtomicU16::new(0);

    let span = RANGE_END - RANGE_START;
    if NEXT.load(Ordering::Relaxed) == 0 {
        let seed = (std::process::id() as u16) % span;
        NEXT.store(seed.max(1), Ordering::Relaxed);
    }

    for _ in 0..MAX_PROBES {
        let port = RANGE_START + (NEXT.fetch_add(1, Ordering::Relaxed) % span);
        // Probe by binding — the only way to learn a port is actually free.
        // Dropping the listener immediately leaves a residual window, which is
        // why the range matters more than the probe.
        if TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
    panic!(
        "no free port in {RANGE_START}..{RANGE_END} after {MAX_PROBES} probes — a previous \
         scenario almost certainly leaked a child process holding them"
    );
}
