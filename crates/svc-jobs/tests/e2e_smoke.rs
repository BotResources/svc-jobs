mod support;

use std::net::TcpListener;
use std::process::Command;
use std::time::{Duration, Instant};

use async_graphql::parser::parse_schema;
use async_graphql::parser::types::{ServiceDocument, TypeKind, TypeSystemDefinition};
use br_test_harness::{E2eDatabase, GraphqlClient, SpawnedProcess};
use reqwest::StatusCode;
use support::fixture::{
    APP_PASSWORD, APP_ROLE, BIN, JobsFixture, free_port, require_provisioned_infrastructure,
};

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

const DECLARED_MUTATIONS: [&str; 3] = ["jobsCancelJob", "jobsDeleteJob", "jobsManualRetryJob"];

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
