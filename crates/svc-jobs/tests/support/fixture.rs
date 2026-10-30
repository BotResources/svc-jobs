use std::time::Duration;

use br_core_auth::Passport;
use br_test_harness::{E2eDatabase, FabricTestNats, GraphqlClient, SpawnedProcess, TestNats};

use super::infra;
pub use super::passports::*;
pub use super::spawn::spawn_instance;
use super::spawn::spawn_process;

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
