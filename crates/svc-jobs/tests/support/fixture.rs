use std::net::TcpListener;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;

use br_core_auth::Passport;
use br_test_harness::{
    E2eDatabase, FabricTestNats, GraphqlClient, PassportBuilder, SpawnedProcess, TestNats,
    recreate_stream,
};
use uuid::Uuid;

use super::wire;

pub const BIN: &str = env!("CARGO_BIN_EXE_svc-jobs");
pub const APP_ROLE: &str = "jobs_app";
pub const BOOT_TIMEOUT: Duration = Duration::from_secs(60);

pub struct Knobs {
    pub inactivity_timeout_seconds: u64,
    pub run_max_duration_seconds: u64,
    pub retry_base_delay_seconds: u64,
    pub max_attempts_ceiling: u32,
    pub backstop_interval_seconds: u64,
}

impl Default for Knobs {
    fn default() -> Self {
        Self {
            inactivity_timeout_seconds: 600,
            run_max_duration_seconds: 600,
            retry_base_delay_seconds: 1,
            max_attempts_ceiling: 3,
            backstop_interval_seconds: 1,
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

    pub async fn start_cluster(instance_count: usize, knobs: Knobs) -> Self {
        require_provisioned_infrastructure();

        let db = E2eDatabase::create(true, &[]).await;
        let app_password = db.db_name().replace('_', "");
        let db = db.with_app_role(APP_ROLE, &app_password).await;

        let fabric = FabricTestNats::start().await;
        let nats = TestNats::setup_on(&fabric.url()).await;
        provision_runner_transport(&nats).await;

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

    pub fn logs(&self) -> String {
        self.instances
            .iter()
            .map(|instance| instance.logs())
            .collect::<Vec<_>>()
            .join("\n")
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

pub fn platform_admin() -> Passport {
    PassportBuilder::new()
        .user_id(Uuid::now_v7())
        .super_admin(true)
        .claim("display_name", "Platform Administrator")
        .claim("email", "admin@botresources.ai")
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

pub fn machine_caller() -> Passport {
    PassportBuilder::new()
        .user_id(Uuid::now_v7())
        .super_admin(true)
        .build_service()
}

async fn provision_runner_transport(nats: &TestNats) {
    let js = nats.jetstream();
    recreate_stream(js, wire::TRIGGER_STREAM, &[wire::TRIGGER_BIND]).await;
    recreate_stream(js, wire::STATUS_STREAM, &[wire::STATUS_BIND]).await;
    recreate_stream(js, wire::LOG_STREAM, &[wire::LOG_BIND]).await;
    nats.create_kv(wire::CANCEL_BUCKET).await;
    nats.create_kv(wire::PRESENCE_BUCKET).await;
}

async fn spawn_instance(
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

    let env: Vec<(&str, &str)> = vec![
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
    ];

    let mut service = SpawnedProcess::spawn(BIN, &[], &env);
    let outcome = service
        .await_boot(&format!("{base_url}/readyz"), BOOT_TIMEOUT)
        .await;
    assert!(
        outcome.is_ready(),
        "svc-jobs never reported ready within {BOOT_TIMEOUT:?} ({outcome:?})\nlogs:\n{}",
        service.logs(),
    );

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
    const RANGE_START: u16 = 20_000;
    const RANGE_END: u16 = 32_000;
    const MAX_PROBES: u16 = 64;

    static NEXT: AtomicU16 = AtomicU16::new(0);

    let span = RANGE_END - RANGE_START;
    if NEXT.load(Ordering::Relaxed) == 0 {
        let seed = (std::process::id() as u16) % span;
        NEXT.store(seed.max(1), Ordering::Relaxed);
    }

    for _ in 0..MAX_PROBES {
        let port = RANGE_START + (NEXT.fetch_add(1, Ordering::Relaxed) % span);
        if TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
    panic!(
        "no free port in {RANGE_START}..{RANGE_END} after {MAX_PROBES} probes — a previous \
         scenario almost certainly leaked a child process holding them"
    );
}
