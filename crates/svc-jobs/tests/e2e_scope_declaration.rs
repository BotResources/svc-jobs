mod support;

use std::collections::BTreeSet;
use std::time::Duration;

use br_core_integration::IntegrationCommand;
use br_core_scope::{DeclareServiceScopes, ScopeDeclarationError, ServiceKey};
use br_scope_declaration_contract::declare_command_coords;
use br_test_harness::{CommandCapture, FabricTestNats, wait_until};
use conformance_scope::{
    AcceptorBehavior, AttachTarget, DEFAULT_TIMEOUT, ExpectedDeclaration, PlatformOnly,
    attach_default, run_attach,
};
use support::fixture::{DeclaringInstance, JobsFixture};
use uuid::Uuid;

const SERVICE: &str = "jobs";
const SCOPE: &str = "jobs:runner";
const QUIET_WINDOW: Duration = Duration::from_secs(3);
const VERDICT_TIMEOUT: Duration = Duration::from_secs(30);

fn expected() -> ExpectedDeclaration {
    ExpectedDeclaration::from_parts(SERVICE, &[SCOPE.to_string()], &PlatformOnly::All(false))
}

fn service_key() -> ServiceKey {
    ServiceKey::new(SERVICE.to_string()).expect("`jobs` is a valid service key")
}

async fn await_declare(capture: &CommandCapture, count: usize) -> Uuid {
    let arrived = wait_until(VERDICT_TIMEOUT, || async { capture.count() >= count }).await;
    assert!(arrived, "Jobs published {} declaration(s)", capture.count());
    *capture
        .correlation_ids()
        .last()
        .expect("a declaration was captured")
}

async fn never_ready(instance: &DeclaringInstance, window: Duration, what: &str) {
    let deadline = tokio::time::Instant::now() + window;
    while tokio::time::Instant::now() < deadline {
        assert_ne!(instance.readyz().await, 200, "{what}: Jobs reported ready");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn capture(fabric: &FabricTestNats) -> CommandCapture {
    let coords = declare_command_coords().expect("the declare coordinates are well formed");
    fabric.capture_commands(&[&coords]).await
}

#[tokio::test]
async fn jobs_declares_its_one_scope_and_is_ready_only_once_identity_accepts() {
    let fixture = JobsFixture::start().await;
    let declares = capture(fixture.fabric()).await;
    let instance = fixture.spawn_declaring();

    let correlation = await_declare(&declares, 1).await;
    let raw = declares
        .for_correlation(correlation)
        .into_iter()
        .next()
        .expect("the declaration payload");
    let command: IntegrationCommand<DeclareServiceScopes> =
        serde_json::from_slice(&raw.payload).expect("the frozen declaration decodes");
    let declaration = command
        .payload
        .validate()
        .expect("the declaration validates");
    expected()
        .assert_matches(&declaration)
        .unwrap_or_else(|mismatch| panic!("the declared content: {mismatch}"));
    let keys: BTreeSet<String> = declaration
        .scopes()
        .iter()
        .map(|scope| scope.key.as_str().to_string())
        .collect();
    assert_eq!(keys, BTreeSet::from([SCOPE.to_string()]));

    never_ready(&instance, QUIET_WINDOW, "before Identity's verdict").await;

    conformance_scope::accept(fixture.fabric().fabric(), &service_key(), correlation)
        .await
        .expect("Identity accepts the declaration");
    let ready = wait_until(VERDICT_TIMEOUT, || async { instance.readyz().await == 200 }).await;
    assert!(ready, "Jobs never turned ready\n{}", instance.logs());

    instance.shutdown().await;
    declares.stop().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_rejected_declaration_is_logged_and_jobs_never_reports_ready() {
    let fixture = JobsFixture::start().await;
    let declares = capture(fixture.fabric()).await;
    let instance = fixture.spawn_declaring();
    let correlation = await_declare(&declares, 1).await;

    conformance_scope::reject(
        fixture.fabric().fabric(),
        &service_key(),
        ScopeDeclarationError::ScopeOwnedByAnotherService {
            key: SCOPE.to_string(),
            owner: "another_service".to_string(),
        },
        correlation,
    )
    .await
    .expect("Identity rejects the declaration");

    never_ready(&instance, QUIET_WINDOW * 2, "after the rejection").await;
    assert!(
        instance.logs().contains("scope_owned_by_another_service"),
        "the reason is logged:\n{}",
        instance.logs()
    );
    instance.shutdown().await;
    declares.stop().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn the_scope_declaration_conformance_battery_passes_against_the_binary() {
    let fixture = JobsFixture::start().await;
    let instance = fixture.spawn_declaring();

    let report = run_attach(
        &AttachTarget {
            nats_url: fixture.fabric().url(),
            readyz_url: format!("{}/readyz", instance.url),
        },
        &expected(),
        &AcceptorBehavior::Accept,
        &attach_default(),
        DEFAULT_TIMEOUT,
    )
    .await
    .expect("the conformance-scope battery runs");
    assert!(
        report.is_conformant(),
        "conformance-scope (attach) must pass: {report:#?}\n{}",
        instance.logs()
    );
    instance.shutdown().await;
    fixture.shutdown().await;
}
