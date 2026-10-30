use serde_json::{Value, json};
use uuid::Uuid;

use super::fixture::{JobsFixture, runner_access};
use super::gesture::{self, Work};
use super::producer::{JobDeclaration, Producer};
use super::runner;
use super::surface::{self, Agent};
use super::tree::{Pair, pair};
use super::{LONG, wire};

pub struct Caller<'a> {
    pub pair: Pair<'a>,
    pub root: Uuid,
    pub trigger: Value,
    pub work: Work,
    pub address: Uuid,
}

pub async fn caller<'a>(fixture: &'a JobsFixture, label: &str) -> Caller<'a> {
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut pair = pair(fixture, label).await;
    let root = JobDeclaration::new(&pair.parent.runner_type).with_max_attempts(1);
    producer.declare_v2(&root).await;
    let trigger = pair.parent.next_trigger(LONG).await;
    pair.parent.start_applied(fixture, &trigger).await;
    let agent = Agent::with_inbox();
    let address = agent.agent_id;
    surface::register(
        fixture.nats(),
        &pair.parent,
        runner::run_id(&trigger),
        &[agent],
    )
    .await;
    Caller {
        work: Work::of(&pair.parent, &trigger),
        pair,
        root: root.job_id,
        trigger,
        address,
    }
}

pub async fn background_call(fixture: &JobsFixture, caller: &mut Caller<'_>) -> Value {
    let producer = Producer::new(fixture.fabric(), "projects");
    producer
        .send_v3(
            wire::VERB_CREATE,
            json!({
                "job_id": Uuid::now_v7().to_string(),
                "runner_type": caller.pair.child.runner_type,
                "producer": "projects",
                "parent_job_id": caller.root.to_string(),
                "mode": "BACKGROUND",
                "caller_address": caller.address.to_string(),
                "acting_run_id": runner::run_id(&caller.trigger).to_string(),
                "acting_instance_key": caller.pair.parent.instance_key,
                "max_attempts": 1,
            }),
        )
        .await;
    caller.pair.child.next_trigger(LONG).await
}

pub async fn notices(fixture: &JobsFixture, work: &Work, address: Option<Uuid>) -> Vec<Value> {
    gesture::items(&gesture::unread(&fixture.gql(), &runner_access(), work, address).await)
        .into_iter()
        .filter(|item| item["itemKind"] == "NOTICE")
        .collect()
}

pub fn types(items: &[Value]) -> Vec<String> {
    items
        .iter()
        .map(|item| item["noticeType"].as_str().unwrap_or_default().to_owned())
        .collect()
}

pub async fn wait_for_notice(
    fixture: &JobsFixture,
    work: &Work,
    address: Option<Uuid>,
    notice_type: &str,
) -> Vec<Value> {
    wait_for_job_notice(fixture, work, address, notice_type, None).await
}

pub async fn wait_for_job_notice(
    fixture: &JobsFixture,
    work: &Work,
    address: Option<Uuid>,
    notice_type: &str,
    job_id: Option<Uuid>,
) -> Vec<Value> {
    let waited = std::time::Instant::now();
    loop {
        let seen = notices(fixture, work, address).await;
        if seen.iter().any(|item| {
            item["noticeType"] == notice_type
                && job_id.is_none_or(|job| item["jobId"] == json!(job.to_string()))
        }) {
            return seen;
        }
        assert!(
            waited.elapsed() < LONG,
            "no {notice_type} notice arrived; saw {seen:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}
