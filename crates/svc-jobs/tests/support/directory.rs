use br_core_auth::Passport;
use serde_json::{Value, json};
use uuid::Uuid;

use super::fixture::JobsFixture;

pub const TREE_AGENTS: &str = "query($jobId: UUID!) { jobsTreeAgents(jobId: $jobId) { \
     agentId runId runnerType state mailAddress restartableNow } }";

pub async fn tree_agents(fixture: &JobsFixture, passport: &Passport, job_id: Uuid) -> Value {
    fixture
        .gql()
        .query(
            passport,
            TREE_AGENTS,
            json!({ "jobId": job_id.to_string() }),
        )
        .await
}

pub fn listed(response: &Value) -> Vec<Value> {
    assert!(
        response["errors"].is_null(),
        "the administrator reads the directory: {response}"
    );
    response["data"]["jobsTreeAgents"]
        .as_array()
        .cloned()
        .unwrap_or_default()
}

pub fn entry_of<'a>(agents: &'a [Value], agent: Uuid, key: &str) -> Option<&'a Value> {
    agents
        .iter()
        .find(|listed| listed[key] == agent.to_string())
}
