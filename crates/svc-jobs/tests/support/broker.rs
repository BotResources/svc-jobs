//! A `nats-server -js` whose process the scenario owns, so it can be stopped and
//! started again on the same endpoint with the same JetStream store — what a NATS
//! pod restarting on its volume, or the node under it coming back, looks like
//! from the service.
//!
//! Only the PROCESS lifecycle lives here: the harness's spawned server cannot be
//! restarted on the same port. Every NATS protocol access still goes through the
//! harness clients (`FabricTestNats`, `TestNats`).

use std::path::{Path, PathBuf};
use std::time::Duration;

use br_test_harness::wait_until;
use tokio::process::{Child, Command};
use uuid::Uuid;

use super::fixture::free_port;

const LOOPBACK: &str = "127.0.0.1";

pub struct RestartableNats {
    child: Option<Child>,
    port: u16,
    store: PathBuf,
}

impl RestartableNats {
    pub async fn start() -> Self {
        let store = std::env::temp_dir().join(format!("svc-jobs-outage-{}", Uuid::now_v7()));
        std::fs::create_dir_all(&store).expect("create the broker's JetStream store dir");
        let port = free_port();
        let child = spawn(port, &store).await;
        Self {
            child: Some(child),
            port,
            store,
        }
    }

    pub fn url(&self) -> String {
        format!("nats://{LOOPBACK}:{}", self.port)
    }

    /// The broker goes away: its process is killed and its port refuses
    /// connections until [`Self::resume`].
    pub async fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill().await;
            let _ = child.wait().await;
        }
    }

    /// The broker comes back on the same port, with the streams, buckets and
    /// durables it held when it stopped.
    pub async fn resume(&mut self) {
        assert!(self.child.is_none(), "the broker is already running");
        self.child = Some(spawn(self.port, &self.store).await);
    }

    pub async fn shutdown(mut self) {
        self.stop().await;
        let _ = std::fs::remove_dir_all(&self.store);
    }
}

async fn spawn(port: u16, store: &Path) -> Child {
    let child = Command::new("nats-server")
        .arg("-js")
        .arg("-a")
        .arg(LOOPBACK)
        .arg("-p")
        .arg(port.to_string())
        .arg("-sd")
        .arg(store)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn nats-server (brew install nats-server, or scripts/provision-e2e-infra.sh)");
    let address = format!("{LOOPBACK}:{port}");
    let listening = wait_until(Duration::from_secs(10), || {
        let address = address.clone();
        async move { tokio::net::TcpStream::connect(&address).await.is_ok() }
    })
    .await;
    assert!(listening, "nats-server never listened on {address}");
    child
}
