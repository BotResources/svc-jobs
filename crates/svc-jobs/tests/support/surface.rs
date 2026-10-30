use br_test_harness::TestNats;
use serde_json::{Value, json};
use uuid::Uuid;

use super::db::Durable;
use super::fixture::JobsFixture;
use super::runner::{self, FakeRunner};

pub struct Agent {
    pub agent_id: Uuid,
    pub has_inbox: bool,
    pub declares_restart: bool,
}

impl Agent {
    pub fn with_inbox() -> Self {
        Self {
            agent_id: Uuid::now_v7(),
            has_inbox: true,
            declares_restart: true,
        }
    }

    pub fn without_inbox() -> Self {
        Self {
            agent_id: Uuid::now_v7(),
            has_inbox: false,
            declares_restart: false,
        }
    }

    pub fn with_id(mut self, agent_id: Uuid) -> Self {
        self.agent_id = agent_id;
        self
    }

    fn wire(&self) -> Value {
        json!({
            "agent_id": self.agent_id.to_string(),
            "has_inbox": self.has_inbox,
            "declares_restart": self.declares_restart,
        })
    }
}

async fn request(nats: &TestNats, subject: String, body: Value) -> Value {
    let reply = tokio::time::timeout(
        super::LONG,
        nats.client().request(subject, body.to_string().into()),
    )
    .await
    .expect("Jobs answers a runner surface request")
    .expect("a runner surface request finds a responder");
    serde_json::from_slice(&reply.payload).expect("a surface reply is JSON")
}

pub async fn register(
    nats: &TestNats,
    runner: &FakeRunner<'_>,
    run_id: Uuid,
    agents: &[Agent],
) -> Value {
    register_as(
        nats,
        &runner.runner_type,
        &runner.instance_key,
        run_id,
        agents,
    )
    .await
}

pub async fn register_as(
    nats: &TestNats,
    runner_type: &str,
    instance_key: &str,
    run_id: Uuid,
    agents: &[Agent],
) -> Value {
    request(
        nats,
        format!("jobs.runner.register_agents.{runner_type}"),
        json!({
            "version": 1,
            "run_id": run_id.to_string(),
            "instance_key": instance_key,
            "agents": agents.iter().map(Agent::wire).collect::<Vec<_>>(),
        }),
    )
    .await
}

pub async fn list(nats: &TestNats, runner: &FakeRunner<'_>, run_id: Uuid) -> Value {
    request(
        nats,
        format!("jobs.runner.list_agents.{}", runner.runner_type),
        json!({
            "version": 1,
            "run_id": run_id.to_string(),
            "instance_key": runner.instance_key,
        }),
    )
    .await
}

pub fn reason(reply: &Value) -> &str {
    assert_eq!(reply["accepted"], false, "the request was refused: {reply}");
    reply["reason"].as_str().unwrap_or_default()
}

pub fn agent_ids(listing: &Value) -> Vec<String> {
    let mut ids: Vec<String> = listing["agents"]
        .as_array()
        .map(|agents| {
            agents
                .iter()
                .filter_map(|agent| agent["agent_id"].as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default();
    ids.sort();
    ids
}

pub async fn start(fixture: &JobsFixture, runner: &FakeRunner<'_>, trigger: &Value) {
    runner.start_run(trigger).await;
    let run = runner::run_id(trigger);
    let durable = Durable::open(&fixture.app_url()).await;
    let waited = tokio::time::Instant::now();
    while durable
        .count("SELECT count(*) FROM run_starts WHERE run_id = $1", run)
        .await
        == 0
    {
        assert!(
            waited.elapsed() < super::LONG,
            "Jobs never applied the start of run {run}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    durable.close().await;
}
