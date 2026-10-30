use std::net::TcpListener;

use br_test_harness::{BootOutcome, E2eDatabase, SpawnedProcess};

use super::fixture::{BIN, BOOT_TIMEOUT, Knobs};

const BIND_ATTEMPTS: u32 = 3;

pub async fn spawn_instance(
    db: &E2eDatabase,
    nats_url: &str,
    knobs: &Knobs,
) -> (SpawnedProcess, String) {
    let mut attempt = 1;
    loop {
        let (mut service, base_url) = spawn_process(db, nats_url, knobs);
        let outcome = service
            .await_boot(&format!("{base_url}/readyz"), BOOT_TIMEOUT)
            .await;
        if outcome.is_ready() {
            return (service, base_url);
        }
        let lost_the_port = matches!(outcome, BootOutcome::Exited(_))
            && service.logs().contains("Address already in use");
        assert!(
            lost_the_port && attempt < BIND_ATTEMPTS,
            "svc-jobs never reported ready within {BOOT_TIMEOUT:?} ({outcome:?})\nlogs:\n{}",
            service.logs(),
        );
        service.shutdown().await;
        attempt += 1;
    }
}

pub(super) fn spawn_process(
    db: &E2eDatabase,
    nats_url: &str,
    knobs: &Knobs,
) -> (SpawnedProcess, String) {
    let owner_url = db.owner_migration_url();
    let app_url = db.app_url();
    let port = free_port();
    let port_text = port.to_string();
    let base_url = format!("http://127.0.0.1:{port}");

    let inactivity = knobs.inactivity_timeout_seconds.to_string();
    let run_max = knobs.run_max_duration_seconds.to_string();
    let retry_base = knobs.retry_base_delay_seconds.to_string();
    let ceiling = knobs.max_attempts_ceiling.to_string();
    let interval = knobs.backstop_interval_seconds.to_string();
    let quiet_period = knobs.retirement_quiet_period_seconds.to_string();
    let impact_interval = knobs.runner_type_impact_interval_seconds.to_string();
    let default_max_priority = knobs.default_max_priority.map(|level| level.to_string());
    let claim_reservation = knobs.claim_reservation_seconds.to_string();
    let claim_max_wait = knobs.claim_max_wait_seconds.to_string();
    let claim_max_held = knobs.claim_max_held.map(|held| held.to_string());
    let restart_initial = knobs
        .task_restart_initial_backoff_milliseconds
        .map(|milliseconds| milliseconds.to_string());
    let restart_max = knobs
        .task_restart_max_backoff_seconds
        .map(|seconds| seconds.to_string());

    let scope_declaration = knobs.scope_declaration.to_string();
    let mut env: Vec<(&str, &str)> = vec![
        ("SCOPE_DECLARATION_ENABLED", scope_declaration.as_str()),
        ("DATABASE_URL", app_url.as_str()),
        ("DATABASE_URL_OWNER", owner_url.as_str()),
        ("HOST", "127.0.0.1"),
        ("PORT", port_text.as_str()),
        ("ENVIRONMENT", "local"),
        ("RUST_LOG", "info"),
        ("NATS_URL", nats_url),
        ("JOBS_INACTIVITY_TIMEOUT_SECONDS", inactivity.as_str()),
        ("JOBS_RUN_MAX_DURATION_SECONDS", run_max.as_str()),
        ("JOBS_RETRY_BASE_DELAY_SECONDS", retry_base.as_str()),
        ("JOBS_MAX_ATTEMPTS_CEILING", ceiling.as_str()),
        ("JOBS_BACKSTOP_INTERVAL_SECONDS", interval.as_str()),
        (
            "JOBS_RETIREMENT_QUIET_PERIOD_SECONDS",
            quiet_period.as_str(),
        ),
        (
            "JOBS_RUNNER_TYPE_IMPACT_INTERVAL_SECONDS",
            impact_interval.as_str(),
        ),
        ("JOBS_CLAIM_RESERVATION_SECONDS", claim_reservation.as_str()),
        ("JOBS_CLAIM_MAX_WAIT_SECONDS", claim_max_wait.as_str()),
    ];

    if let Some(level) = default_max_priority.as_deref() {
        env.push(("JOBS_DEFAULT_MAX_PRIORITY", level));
    }
    if let Some(held) = claim_max_held.as_deref() {
        env.push(("JOBS_CLAIM_MAX_HELD", held));
    }
    if let Some(initial) = restart_initial.as_deref() {
        env.push(("JOBS_TASK_RESTART_INITIAL_BACKOFF_MILLISECONDS", initial));
    }
    if let Some(max) = restart_max.as_deref() {
        env.push(("JOBS_TASK_RESTART_MAX_BACKOFF_SECONDS", max));
    }

    let service = SpawnedProcess::spawn(BIN, &[], &env);
    (service, base_url)
}

pub fn free_port() -> u16 {
    TcpListener::bind(("127.0.0.1", 0))
        .and_then(|listener| listener.local_addr())
        .map(|address| address.port())
        .expect("the operating system hands out a free local port")
}
