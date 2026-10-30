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

const LOOPBACK: &str = "127.0.0.1";
const LISTENING: &str = "Listening for client connections on ";
const ANY_FREE_PORT: &str = "-1";

pub struct RestartableNats {
    child: Option<Child>,
    port: u16,
    store: PathBuf,
}

impl RestartableNats {
    pub async fn start() -> Self {
        let store = std::env::temp_dir().join(format!("svc-jobs-outage-{}", Uuid::now_v7()));
        std::fs::create_dir_all(&store).expect("create the broker's JetStream store dir");
        let (child, port) = spawn(ANY_FREE_PORT, &store).await;
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
        let (child, port) = spawn(&self.port.to_string(), &self.store).await;
        assert_eq!(port, self.port, "the broker resumes on its own endpoint");
        self.child = Some(child);
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

fn announced_port(log: &Path) -> Option<u16> {
    let text = std::fs::read_to_string(log).ok()?;
    let line = text.lines().find(|line| line.contains(LISTENING))?;
    line.rsplit(':').next()?.trim().parse().ok()
}

async fn spawn(port: &str, store: &Path) -> (Child, u16) {
    let log = log_of(store);
    let _ = std::fs::remove_file(&log);
    let mut child = Command::new("nats-server")
        .arg("-js")
        .arg("-a")
        .arg(LOOPBACK)
        .arg("-p")
        .arg(port)
        .arg("-sd")
        .arg(store)
        .arg("-l")
        .arg(&log)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn nats-server (brew install nats-server, or scripts/provision-e2e-infra.sh)");
    let listening = wait_until(Duration::from_secs(10), || {
        let exited = child.try_wait().ok().flatten().is_some();
        let announced = announced_port(&log);
        async move { exited || announced.is_some() }
    })
    .await;
    let announced = announced_port(&log);
    assert!(
        listening && announced.is_some(),
        "nats-server never announced a listening port (asked for {port}): {}",
        std::fs::read_to_string(&log).unwrap_or_default()
    );
    (child, announced.expect("the port was announced"))
}
