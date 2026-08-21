mod support;

use std::net::TcpListener;
use std::process::Command;
use std::time::{Duration, Instant};

use async_graphql::parser::parse_schema;
use async_graphql::parser::types::{ServiceDocument, TypeKind, TypeSystemDefinition};
use br_test_harness::{E2eDatabase, GraphqlClient, SpawnedProcess, SseSubscription};
use reqwest::StatusCode;
use serde_json::json;
use support::fixture::{
    APP_PASSWORD, APP_ROLE, BIN, JobsFixture, free_port, require_provisioned_infrastructure,
};
use support::runner::FakeRunner;
use support::{FLEET_CHANGED, QUIET, SHORT, gql, stream, subs, wire};

const ROOT_FIELD_PREFIX: &str = "jobs";

const DOWN_PHASE_WINDOW: Duration = Duration::from_secs(2);
const DOWN_PHASE_PROBE_INTERVAL: Duration = Duration::from_millis(50);

const DECLARED_QUERIES: [&str; 5] = [
    "jobs",
    "jobsFleet",
    "jobsJob",
    "jobsJobBySource",
    "jobsLogs",
];

const DECLARED_MUTATIONS: [&str; 6] = [
    "jobsCancelJob",
    "jobsDeleteJob",
    "jobsDeprecateRunnerType",
    "jobsManualRetryJob",
    "jobsReactivateRunnerType",
    "jobsRetireRunnerType",
];

const DECLARED_SUBSCRIPTIONS: [&str; 4] = [
    "jobsChanged",
    "jobsFleetChanged",
    "jobsJobChanged",
    "jobsJobLogTail",
];

#[tokio::test]
async fn operational_probes_and_the_published_schema() {
    let printed = schema_subcommand_stdout();
    assert_no_log_line_precedes_the_document(&printed);
    let document = parse_or_panic(&printed);
    assert_every_root_field_carries_the_bounded_context_prefix(&document);
    assert_the_published_root_fields_are_exactly_the_declared_ones(&document);

    let fixture = JobsFixture::start().await;
    let gql = GraphqlClient::new(fixture.url());

    let (status, _) = gql.get_raw("/livez").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "/livez must be 200 on a running process — the kubelet restarts the pod on anything else",
    );

    let (status, _) = gql.get_raw("/readyz").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "/readyz must be 200 once every declared dependency is bound",
    );

    let (status, body) = gql.get_raw("/metrics").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "/metrics must be 200 — Prometheus scrapes it and a non-200 is a service with no telemetry",
    );
    assert!(
        body.contains("http_requests_total"),
        "/metrics must render the HTTP series the observability layer describes at boot:\n{body}",
    );

    let (status, served_sdl) = gql.get_raw("/sdl").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "/sdl must be 200 — the gateway's composer cannot build a supergraph without it",
    );

    let printed_document = printed.strip_suffix('\n').unwrap_or(&printed);
    assert_eq!(
        served_sdl, printed_document,
        "the schema served at /sdl and the schema printed by `svc-jobs schema` must be the same \
         document — the composer reads the first and CD poses the second",
    );

    fixture.shutdown().await;
}

#[tokio::test]
async fn a_pod_that_reports_ready_delivers_a_first_presence_signal_promptly() {
    // Given: a pod subscribed to at the very first moment it answers /readyz 200, with no settling
    // wait — a pod that were still binding its fan-out or its presence watch would have to make up
    // the delay somewhere inside the bound asserted below
    let fixture = JobsFixture::start().await;
    assert!(
        gql::ready(fixture.url()).await,
        "the fixture returns once the pod reports ready, so this scenario starts on the first \
         200\nlogs:\n{}",
        fixture.logs(),
    );

    let admin = fixture.admin();
    let runner_type = wire::unique_runner_type("readiness");
    let mut watch =
        SseSubscription::open(fixture.url(), admin, &subs::fleet_changed(&runner_type)).await;
    stream::snapshot(&mut watch, FLEET_CHANGED, SHORT).await;

    // When: an instance of a never-seen type announces presence on the cold pod
    let mut instance = FakeRunner::new(fixture.nats(), &runner_type, "instance-a");
    let announced_at = Instant::now();
    instance.connect().await;

    // Then: the registration delta travels presence KV → durable fan-out → subscriber inside a
    // tight bound. The presence watch replays history, so a late-bound watch would still deliver
    // eventually — what a generous budget could never distinguish from a healthy pod. The bound is
    // the assertion: a pod that reports ready before its fan-out is bound cannot meet it
    let registered = stream::await_fleet_event(&mut watch, wire::KIND_TYPE_REGISTERED, SHORT).await;
    let elapsed = announced_at.elapsed();
    assert!(
        elapsed < SHORT,
        "the first presence signal reached the subscriber in {elapsed:?}, outside the {SHORT:?} \
         budget a pod that answered /readyz owes — readiness that does not already mean 'fan-out \
         and presence watch bound' is a pod taking traffic it cannot serve\nlogs:\n{}",
        fixture.logs(),
    );
    assert_eq!(
        registered["runnerType"]["typeKey"],
        json!(runner_type),
        "the delta the newly-ready pod pushes is the authoritative projection of the type that \
         just registered: {registered}",
    );

    fixture.shutdown().await;
}

#[tokio::test]
async fn a_silence_proof_fails_rather_than_passes_when_the_socket_dies() {
    // Given: a subscriber holding an open fleet stream
    let fixture = JobsFixture::start().await;
    let runner_type = wire::unique_runner_type("dead-stream");
    let mut watch = SseSubscription::open(
        fixture.url(),
        fixture.admin(),
        &subs::fleet_changed(&runner_type),
    )
    .await;
    stream::snapshot(&mut watch, FLEET_CHANGED, SHORT).await;

    // When: the service goes away under it, and absence is proved on that stream alone — nothing
    // else runs inside this boundary, so a set-up failure can never be mistaken for the detector
    fixture.shutdown().await;
    let outcome = tokio::spawn(async move {
        stream::expect_total_silence(&mut watch, "a stream the service no longer serves", QUIET)
            .await;
    })
    .await;

    // Then: proving absence on a dead stream must fail loudly. Every `expect no delta` in this
    // suite rests on this: a dead stream delivers nothing, so it satisfies any silence check by
    // construction and turns every absence proof built on it into a vacuous pass
    assert!(
        outcome.is_err(),
        "expect_total_silence accepted a stream the service had stopped serving, so every absence \
         this suite proves on a subscription would be worth nothing",
    );
    let message = panic_message(outcome.expect_err("the silence proof panicked"));
    assert!(
        message.contains("subscription stream errored")
            || message.contains("the service ended the subscription"),
        "the failure must name the dead stream, not something incidental: {message}",
    );
}

fn panic_message(failure: tokio::task::JoinError) -> String {
    assert!(
        failure.is_panic(),
        "the silence proof must fail by assertion, not by cancellation: {failure}"
    );
    let payload = failure.into_panic();
    payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| {
            payload
                .downcast_ref::<&str>()
                .map(|text| (*text).to_string())
        })
        .unwrap_or_default()
}

#[test]
fn a_stream_that_ends_with_budget_left_is_a_closed_stream_not_a_timeout() {
    // Given: a four-second wait, whose end-of-stream margin is the 500ms ceiling
    let budget = Duration::from_secs(4);
    let margin = Duration::from_millis(500);

    // Then: an end arriving while a real slice of the budget is unspent is the service closing the
    // stream. This is the branch that guards every silence proof, and the one a killed socket never
    // reaches, because the transport errors before the stream can end cleanly
    assert!(stream::is_stream_end(budget, Duration::from_secs(2)));
    assert!(stream::is_stream_end(
        budget,
        margin + Duration::from_millis(1)
    ));

    // Then: an end arriving with the budget spent is an ordinary timeout, and the silence is real
    assert!(!stream::is_stream_end(budget, margin));
    assert!(!stream::is_stream_end(budget, Duration::from_millis(100)));
    assert!(!stream::is_stream_end(budget, Duration::ZERO));
}

#[test]
fn the_stream_end_margin_scales_with_the_budget_up_to_a_ceiling() {
    // Then: short windows get a proportional margin, so the detector never goes inert on them
    assert_eq!(
        stream::margin_for(Duration::from_millis(400)),
        Duration::from_millis(100),
    );
    assert_eq!(
        stream::margin_for(Duration::from_millis(40)),
        Duration::from_millis(10),
    );

    // Then: long windows are capped, so a generous budget does not blind the detector to a stream
    // that ends half a second before the deadline
    assert_eq!(
        stream::margin_for(Duration::from_secs(4)),
        Duration::from_millis(500),
    );
    assert_eq!(
        stream::margin_for(Duration::from_secs(20)),
        Duration::from_millis(500),
    );
}

#[tokio::test]
async fn readiness_stays_down_while_a_declared_dependency_is_unbound() {
    // Given: a real database, and a NATS endpoint that takes the connection and never answers
    require_provisioned_infrastructure();
    let db = E2eDatabase::create(true, &[])
        .await
        .with_app_role(APP_ROLE, APP_PASSWORD)
        .await;
    let silent_nats = TcpListener::bind(("127.0.0.1", 0)).expect("a local socket to hold open");
    let nats_url = format!(
        "nats://{}",
        silent_nats
            .local_addr()
            .expect("the held socket has an address"),
    );
    let owner_url = db.owner_migration_url();
    let app_url = db.app_url();
    let port = free_port().to_string();
    let base_url = format!("http://127.0.0.1:{port}");

    // When: the binary boots against it and is probed from the moment it binds its port
    let service = SpawnedProcess::spawn(
        BIN,
        &[],
        &[
            ("DATABASE_URL", app_url.as_str()),
            ("DATABASE_URL_OWNER", owner_url.as_str()),
            ("HOST", "127.0.0.1"),
            ("PORT", port.as_str()),
            ("ENVIRONMENT", "local"),
            ("RUST_LOG", "info"),
            ("NATS_URL", nats_url.as_str()),
        ],
    );

    // Then: every answer it gives is 503 with a reason, and it never once reports ready
    let deadline = Instant::now() + DOWN_PHASE_WINDOW;
    let mut answers = 0_usize;
    while Instant::now() < deadline {
        if let Ok(response) = reqwest::get(format!("{base_url}/readyz")).await {
            let status = response.status();
            let reason = response.text().await.unwrap_or_default();
            assert_eq!(
                status,
                StatusCode::SERVICE_UNAVAILABLE,
                "/readyz answered {status} while the declared NATS infrastructure was unbound — a \
                 pod that reports ready before its dependencies are bound takes traffic it cannot \
                 serve\nlogs:\n{}",
                service.logs(),
            );
            assert!(
                !reason.trim().is_empty(),
                "a NOT READY answer must name what is missing, so an operator reads the culprit \
                 off the probe",
            );
            answers += 1;
        }
        tokio::time::sleep(DOWN_PHASE_PROBE_INTERVAL).await;
    }
    assert!(
        answers > 0,
        "/readyz never answered within {DOWN_PHASE_WINDOW:?}, so the down phase was never \
         observed — the probe proves nothing unless the process served it\nlogs:\n{}",
        service.logs(),
    );

    service.shutdown().await;
    db.cleanup().await;
}

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
        "`svc-jobs schema` printed nothing, so CD would pose an empty document on the registry \
         PatchVersion and the gateway would compose a supergraph without this subgraph",
    );

    stdout
}

fn assert_no_log_line_precedes_the_document(printed: &str) {
    let first = printed
        .lines()
        .find(|line| !line.trim().is_empty())
        .expect("a non-empty document has a first non-empty line");

    assert!(
        !first.trim_start().starts_with('{'),
        "the first line of `svc-jobs schema` looks like a JSON log record, not SDL — something \
         logs before the `schema` branch in main.rs prints the document: {first}",
    );
}

fn parse_or_panic(printed: &str) -> ServiceDocument {
    parse_schema(printed).unwrap_or_else(|error| {
        panic!(
            "`svc-jobs schema` must print a document the registry and the gateway can parse: \
             {error}\n---\n{printed}\n---"
        )
    })
}

fn assert_every_root_field_carries_the_bounded_context_prefix(document: &ServiceDocument) {
    let roots = root_types(document);

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

fn assert_the_published_root_fields_are_exactly_the_declared_ones(document: &ServiceDocument) {
    let roots = root_types(document);

    for (root, declared) in [
        (&roots.query, DECLARED_QUERIES.to_vec()),
        (&roots.mutation, DECLARED_MUTATIONS.to_vec()),
        (&roots.subscription, DECLARED_SUBSCRIPTIONS.to_vec()),
    ] {
        let mut published = fields_of(document, root);
        published.sort();
        let mut expected: Vec<String> = declared.iter().map(|name| name.to_string()).collect();
        expected.sort();
        assert_eq!(
            published, expected,
            "the published `{root}` fields must be exactly the ones the registry declares for \
             this major — a field the registry does not know is an undeclared contract, and a \
             missing one is a contract the service stopped serving",
        );
    }
}

struct RootTypes {
    query: String,
    mutation: String,
    subscription: String,
}

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
