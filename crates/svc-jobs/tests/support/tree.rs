use serde_json::json;
use uuid::Uuid;

use super::fixture::JobsFixture;
use super::producer::Producer;
use super::runner::{self, FakeRunner};
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
