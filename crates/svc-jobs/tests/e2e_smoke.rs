mod support;

use std::process::Command;

use async_graphql::parser::parse_schema;
use async_graphql::parser::types::{ServiceDocument, TypeKind, TypeSystemDefinition};
use br_test_harness::GraphqlClient;
use reqwest::StatusCode;
use support::fixture::{BIN, JobsFixture};

const ROOT_FIELD_PREFIX: &str = "jobs";

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
