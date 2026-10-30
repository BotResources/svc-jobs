use serde_json::json;
use uuid::Uuid;

use super::fixture::JobsFixture;
use super::gesture::Work;
use super::producer::{JobDeclaration, Producer};
use super::runner::{self, FakeRunner};
use super::surface::{self, Agent};
use super::{LONG, dispatch, wire};

pub struct Pair<'a> {
    pub parent: FakeRunner<'a>,
    pub child: FakeRunner<'a>,
}

pub async fn pair<'a>(fixture: &'a JobsFixture, label: &str) -> Pair<'a> {
    let client = fixture.gql();
    let admin = fixture.admin();
    let mut parent = FakeRunner::new(
        fixture.nats(),
        &wire::unique_runner_type(&format!("{label}-parent")),
        "instance-a",
    )
    .declaring_capacity(3);
    let mut child = FakeRunner::new(
        fixture.nats(),
        &wire::unique_runner_type(&format!("{label}-child")),
        "instance-a",
    )
    .declaring_capacity(3);
    parent.connect().await;
    child.connect().await;
    dispatch::await_live_instances(&client, admin, &parent.runner_type, 1, LONG).await;
    dispatch::await_live_instances(&client, admin, &child.runner_type, 1, LONG).await;
    Pair { parent, child }
}

pub async fn call_child(
    fixture: &JobsFixture,
    pair: &mut Pair<'_>,
    parent_job: Uuid,
    parent_run: Uuid,
) -> serde_json::Value {
    let producer = Producer::new(fixture.fabric(), "projects");
    let job_id = Uuid::now_v7();
    producer
        .send_v3(
            wire::VERB_CREATE,
            json!({
                "job_id": job_id.to_string(),
                "runner_type": pair.child.runner_type,
                "producer": "projects",
                "parent_job_id": parent_job.to_string(),
                "mode": "SYNC",
                "acting_run_id": parent_run.to_string(),
                "acting_instance_key": pair.parent.instance_key,
            }),
        )
        .await;
    let trigger = pair.child.next_trigger(LONG).await;
    assert_eq!(runner::job_id(&trigger), job_id);
    trigger
}

pub struct Correspondents<'a> {
    pub pair: Pair<'a>,
    pub root: Uuid,
    pub parent_trigger: serde_json::Value,
    pub child_trigger: serde_json::Value,
    pub parent_work: Work,
    pub child_work: Work,
    pub sender: Uuid,
    pub receiver: Uuid,
    pub bystander: Uuid,
}

pub async fn correspondents<'a>(fixture: &'a JobsFixture, label: &str) -> Correspondents<'a> {
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut pair = pair(fixture, label).await;
    let nats = fixture.nats();
    let root = JobDeclaration::new(&pair.parent.runner_type).with_max_attempts(1);
    producer.declare_v2(&root).await;
    let parent_trigger = pair.parent.next_trigger(LONG).await;
    pair.parent.start_applied(fixture, &parent_trigger).await;
    let sender = Agent::with_inbox();
    let sender_id = sender.agent_id;
    surface::register(
        nats,
        &pair.parent,
        runner::run_id(&parent_trigger),
        &[sender],
    )
    .await;
    let child_trigger = call_child(
        fixture,
        &mut pair,
        root.job_id,
        runner::run_id(&parent_trigger),
    )
    .await;
    pair.child.start_applied(fixture, &child_trigger).await;
    let (receiver, bystander) = (Agent::with_inbox(), Agent::with_inbox());
    let (receiver_id, bystander_id) = (receiver.agent_id, bystander.agent_id);
    surface::register(
        nats,
        &pair.child,
        runner::run_id(&child_trigger),
        &[receiver, bystander],
    )
    .await;
    Correspondents {
        parent_work: Work::of(&pair.parent, &parent_trigger),
        child_work: Work::of(&pair.child, &child_trigger),
        pair,
        root: root.job_id,
        parent_trigger,
        child_trigger,
        sender: sender_id,
        receiver: receiver_id,
        bystander: bystander_id,
    }
}
