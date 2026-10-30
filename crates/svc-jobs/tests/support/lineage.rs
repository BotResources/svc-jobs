use serde_json::{Value, json};
use uuid::Uuid;

use super::fixture::{JobsFixture, runner_access};
use super::gesture::{self, Mail, Work};
use super::producer::Producer;
use super::runner::{self, FakeRunner};
use super::surface::{self, Agent};
use super::{LONG, dispatch, wire};

pub async fn live_runner<'a>(fixture: &'a JobsFixture, label: &str) -> FakeRunner<'a> {
    let mut runner = FakeRunner::new(
        fixture.nats(),
        &wire::unique_runner_type(label),
        "instance-a",
    )
    .declaring_capacity(3);
    runner.connect().await;
    dispatch::await_live_instances(
        &fixture.gql(),
        fixture.admin(),
        &runner.runner_type,
        1,
        LONG,
    )
    .await;
    runner
}

pub async fn started_with_agent(
    fixture: &JobsFixture,
    runner: &mut FakeRunner<'_>,
) -> (Value, Uuid) {
    let trigger = runner.next_trigger(LONG).await;
    runner.start_applied(fixture, &trigger).await;
    let agent = Agent::with_inbox();
    let id = agent.agent_id;
    surface::register(fixture.nats(), runner, runner::run_id(&trigger), &[agent]).await;
    (trigger, id)
}

pub async fn create(
    fixture: &JobsFixture,
    acting: &Work,
    runner_type: &str,
    parent: Uuid,
    extra: Value,
) -> Uuid {
    let job = Uuid::now_v7();
    let mut body = json!({
        "job_id": job.to_string(),
        "runner_type": runner_type,
        "producer": "projects",
        "parent_job_id": parent.to_string(),
        "acting_run_id": acting.run_id.to_string(),
        "acting_instance_key": acting.instance_key,
    });
    body.as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    Producer::new(fixture.fabric(), "projects")
        .send_v3(wire::VERB_CREATE, body)
        .await;
    job
}

pub async fn mail(fixture: &JobsFixture, work: &Work, from: Uuid, to: Uuid) {
    let sent = gesture::send_mail(
        &fixture.gql(),
        &runner_access(),
        work,
        &Mail::new(from, &[to]),
    )
    .await;
    gesture::accepted(&sent, "jobsSendMail");
}

pub async fn act(fixture: &JobsFixture, verb: &str, job: Uuid, acting: &Work) {
    Producer::new(fixture.fabric(), "projects")
        .send_v3(
            verb,
            json!({
                "job_id": job.to_string(),
                "acting_run_id": acting.run_id.to_string(),
                "acting_instance_key": acting.instance_key,
            }),
        )
        .await;
}
