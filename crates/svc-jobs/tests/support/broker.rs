use std::path::{Path, PathBuf};
use std::time::Duration;

use br_test_harness::wait_until;
use tokio::process::{Child, Command};
use uuid::Uuid;

const LOOPBACK: &str = "127.0.0.1";
const LISTENING: &str = "Listening for client connections on ";
const FIRST_PORT: u16 = 20_000;
const PORT_SPAN: u16 = 12_000;
const START_ATTEMPTS: u32 = 8;

pub struct RestartableNats {
    child: Option<Child>,
    port: u16,
    store: PathBuf,
}

impl RestartableNats {
    pub async fn start() -> Self {
        let store = std::env::temp_dir().join(format!("svc-jobs-outage-{}", Uuid::now_v7()));
        std::fs::create_dir_all(&store).expect("create the broker's JetStream store dir");
        for _ in 0..START_ATTEMPTS {
            let port = FIRST_PORT
                + u16::try_from(Uuid::now_v7().as_u128() % u128::from(PORT_SPAN)).unwrap_or(0);
            if let Some(child) = spawn(port, &store).await {
                return Self {
                    child: Some(child),
                    port,
                    store,
                };
            }
        }
        panic!("nats-server never bound one of {START_ATTEMPTS} ports taken from {FIRST_PORT}..");
    }

    pub fn url(&self) -> String {
        format!("nats://{LOOPBACK}:{}", self.port)
    }

    pub async fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill().await;
            let _ = child.wait().await;
        }
    }

    pub async fn resume(&mut self) {
        assert!(self.child.is_none(), "the broker is already running");
        self.child = Some(
            spawn(self.port, &self.store)
                .await
                .expect("the broker resumes on its own endpoint"),
        );
    }

    pub async fn shutdown(mut self) {
        self.stop().await;
        let _ = std::fs::remove_file(log_of(&self.store));
        let _ = std::fs::remove_dir_all(&self.store);
    }
}

fn log_of(store: &Path) -> PathBuf {
    store.with_extension("log")
}

fn announced(log: &Path, port: u16) -> bool {
    std::fs::read_to_string(log)
        .is_ok_and(|text| text.contains(&format!("{LISTENING}{LOOPBACK}:{port}")))
}

async fn spawn(port: u16, store: &Path) -> Option<Child> {
    let log = log_of(store);
    let _ = std::fs::remove_file(&log);
    let mut child = Command::new("nats-server")
        .arg("-js")
        .arg("-a")
        .arg(LOOPBACK)
        .arg("-p")
        .arg(port.to_string())
        .arg("-sd")
        .arg(store)
        .arg("-l")
        .arg(&log)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn nats-server (brew install nats-server, or scripts/provision-e2e-infra.sh)");
    let settled = wait_until(Duration::from_secs(10), || {
        let exited = child.try_wait().ok().flatten().is_some();
        let listening = announced(&log, port);
        async move { exited || listening }
    })
    .await;
    assert!(
        settled,
        "nats-server neither listened nor exited on port {port}: {}",
        std::fs::read_to_string(&log).unwrap_or_default()
    );
    announced(&log, port).then_some(child)
}
