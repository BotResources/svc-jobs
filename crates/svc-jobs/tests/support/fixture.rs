use std::net::TcpListener;
use std::time::Duration;

use br_core_auth::Passport;
use br_test_harness::{
    BootOutcome, E2eDatabase, FabricTestNats, GraphqlClient, PassportBuilder, SpawnedProcess,
    TestNats,
};
use uuid::Uuid;

use super::infra;

pub const BIN: &str = env!("CARGO_BIN_EXE_svc-jobs");
pub const APP_ROLE: &str = "jobs_app";
pub const APP_PASSWORD: &str = "jobsapproleforeveryworktree";
pub const BOOT_TIMEOUT: Duration = Duration::from_secs(60);

pub struct Knobs {
    pub inactivity_timeout_seconds: u64,
    pub run_max_duration_seconds: u64,
    pub retry_base_delay_seconds: u64,
    pub max_attempts_ceiling: u32,
    pub backstop_interval_seconds: u64,
    pub retirement_quiet_period_seconds: u64,
    pub runner_type_impact_interval_seconds: u64,
    pub default_max_priority: Option<i64>,
    pub claim_reservation_seconds: u64,
    pub claim_max_wait_seconds: u64,
    pub claim_max_held: Option<u64>,
    pub scope_declaration: bool,
    pub task_restart_initial_backoff_milliseconds: Option<u64>,
    pub task_restart_max_backoff_seconds: Option<u64>,
    pub presence_ttl: Duration,
}

impl Default for Knobs {
    fn default() -> Self {
        Self {
            inactivity_timeout_seconds: 600,
            run_max_duration_seconds: 600,
            retry_base_delay_seconds: 1,
            max_attempts_ceiling: 3,
            backstop_interval_seconds: 1,
            retirement_quiet_period_seconds: 86_400,
            runner_type_impact_interval_seconds: 60,
            default_max_priority: None,
            claim_reservation_seconds: 600,
            claim_max_wait_seconds: 30,
            claim_max_held: None,
            scope_declaration: false,
            task_restart_initial_backoff_milliseconds: None,
            task_restart_max_backoff_seconds: None,
            presence_ttl: infra::STEADY_PRESENCE_TTL,
        }
    }
}

pub struct JobsFixture {
    db: Option<E2eDatabase>,
    fabric: Option<FabricTestNats>,
    nats: Option<TestNats>,
    instances: Vec<SpawnedProcess>,
    urls: Vec<String>,
    admin: Passport,
    member: Passport,
}

impl JobsFixture {
    pub async fn start() -> Self {
        Self::start_cluster(1, Knobs::default()).await
    }

    pub async fn start_with(knobs: Knobs) -> Self {
        Self::start_cluster(1, knobs).await
    }

    pub async fn start_on(db: E2eDatabase, knobs: Knobs) -> Self {
        Self::start_cluster_on(db, 1, knobs).await
    }

    pub async fn start_cluster(instance_count: usize, knobs: Knobs) -> Self {
        require_provisioned_infrastructure();

        let db = E2eDatabase::create(true, &[])
            .await
            .with_app_role(APP_ROLE, APP_PASSWORD)
            .await;

        Self::start_cluster_on(db, instance_count, knobs).await
    }

    async fn start_cluster_on(db: E2eDatabase, instance_count: usize, knobs: Knobs) -> Self {
        require_provisioned_infrastructure();

        let fabric = FabricTestNats::start()
            .await
            .with_published_language()
            .await;
        let nats = TestNats::setup_on(&fabric.url()).await;
        infra::provision_runner_transport(&nats, knobs.presence_ttl).await;

        let mut instances = Vec::new();
        let mut urls = Vec::new();
        for _ in 0..instance_count {
            let (service, url) = spawn_instance(&db, &fabric.url(), &knobs).await;
            instances.push(service);
            urls.push(url);
        }

        Self {
            db: Some(db),
            fabric: Some(fabric),
            nats: Some(nats),
            instances,
            urls,
            admin: platform_admin(),
            member: ordinary_member(),
        }
    }

    pub fn url(&self) -> &str {
        &self.urls[0]
    }

    pub fn url_of(&self, index: usize) -> &str {
        &self.urls[index]
    }

    pub fn app_url(&self) -> String {
        self.db
            .as_ref()
            .expect("the database is live until shutdown")
            .app_url()
    }

    pub fn gql(&self) -> GraphqlClient {
        GraphqlClient::new(self.url())
    }

    pub fn gql_of(&self, index: usize) -> GraphqlClient {
        GraphqlClient::new(self.url_of(index))
    }

    pub fn admin(&self) -> &Passport {
        &self.admin
    }

    pub fn member(&self) -> &Passport {
        &self.member
    }

    pub fn fabric(&self) -> &FabricTestNats {
        self.fabric.as_ref().expect("fabric is live until shutdown")
    }

    pub fn nats(&self) -> &TestNats {
        self.nats
            .as_ref()
            .expect("runner transport is live until shutdown")
    }

    pub fn spawn_declaring(&self) -> DeclaringInstance {
        let db = self
            .db
            .as_ref()
            .expect("the database is live until shutdown");
        let fabric = self
            .fabric
            .as_ref()
            .expect("the fabric is live until shutdown");
        let knobs = Knobs {
            scope_declaration: true,
            ..Knobs::default()
        };
        let (process, url) = spawn_process(db, &fabric.url(), &knobs);
        DeclaringInstance { process, url }
    }

    pub fn logs(&self) -> String {
        self.instances
            .iter()
            .map(|instance| instance.logs())
            .collect::<Vec<_>>()
            .join("\n")
    }

    pub async fn restart_first(&mut self, knobs: &Knobs) {
        let previous = self.instances.remove(0);
        previous.shutdown().await;
        self.urls.remove(0);

        let db = self
            .db
            .as_ref()
            .expect("the database is live until shutdown");
        let fabric = self
            .fabric
            .as_ref()
            .expect("the fabric is live until shutdown");
        let (service, url) = spawn_instance(db, &fabric.url(), knobs).await;
        self.instances.insert(0, service);
        self.urls.insert(0, url);
    }

    pub async fn shutdown(mut self) {
        for instance in std::mem::take(&mut self.instances) {
            instance.shutdown().await;
        }
        if let Some(nats) = self.nats.take() {
            nats.cleanup().await;
        }
        if let Some(fabric) = self.fabric.take() {
            fabric.shutdown().await;
        }
        if let Some(db) = self.db.take() {
            db.cleanup().await;
        }
    }
}

pub struct DeclaringInstance {
    process: SpawnedProcess,
    pub url: String,
}

impl DeclaringInstance {
    pub async fn readyz(&self) -> u16 {
        GraphqlClient::new(&self.url)
            .get_raw("/readyz")
            .await
            .0
            .as_u16()
    }

    pub fn logs(&self) -> String {
        self.process.logs()
    }

    pub async fn shutdown(self) {
        self.process.shutdown().await;
    }
}

pub fn platform_admin() -> Passport {
    PassportBuilder::new()
        .user_id(Uuid::now_v7())
        .super_admin(true)
        .claim("display_name", "Platform Administrator")
        .claim("email", "admin@botresources.ai")
        .build()
}

pub fn deactivated_super_user() -> Passport {
    PassportBuilder::new()
        .user_id(Uuid::now_v7())
        .super_admin(true)
        .active(false)
        .claim("display_name", "Deactivated Administrator")
        .build()
}

pub fn ordinary_member() -> Passport {
    PassportBuilder::new()
        .user_id(Uuid::now_v7())
        .super_admin(false)
        .claim("display_name", "Ordinary Member")
        .claim("email", "member@botresources.ai")
        .claim("org_id", Uuid::now_v7().to_string())
        .build()
}

pub fn impersonating_member(admin: &Passport) -> Passport {
    PassportBuilder::new()
        .user_id(Uuid::now_v7())
        .super_admin(false)
        .impersonator(admin.actor_id())
        .claim("display_name", "Escalation Attempt")
        .build()
}

pub fn impersonating_admin() -> (Passport, Uuid) {
    let operator_id = Uuid::now_v7();
    let passport = PassportBuilder::new()
        .user_id(Uuid::now_v7())
        .super_admin(true)
        .impersonator(operator_id)
        .claim("display_name", "Impersonated Administrator")
        .build();
    (passport, operator_id)
}

pub fn runner_access() -> Passport {
    PassportBuilder::new()
        .user_id(Uuid::now_v7())
        .claim("scopes", vec!["jobs:runner"])
        .build_service()
}

pub fn machine_caller() -> Passport {
    PassportBuilder::new()
        .user_id(Uuid::now_v7())
        .super_admin(true)
        .build_service()
}

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

fn spawn_process(db: &E2eDatabase, nats_url: &str, knobs: &Knobs) -> (SpawnedProcess, String) {
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

pub fn require_provisioned_infrastructure() {
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

pub fn free_port() -> u16 {
    TcpListener::bind(("127.0.0.1", 0))
        .and_then(|listener| listener.local_addr())
        .map(|address| address.port())
        .expect("the operating system hands out a free local port")
}
