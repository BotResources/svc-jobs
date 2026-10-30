use serde_json::{Value, json};
use uuid::Uuid;

use super::fixture::JobsFixture;
use super::gesture::Work;
use super::producer::{JobDeclaration, Producer};
use super::runner::{self, FakeRunner};
use super::surface::{self, Agent};
use super::{LONG, dispatch, wire};

pub struct CallScene<'a> {
    pub caller: FakeRunner<'a>,
    pub called: FakeRunner<'a>,
    pub root: Uuid,
    pub trigger: Value,
    pub work: Work,
    pub address: Uuid,
}

pub async fn call_scene<'a>(
    fixture: &'a JobsFixture,
    label: &str,
    called_capacity: u32,
) -> CallScene<'a> {
    let client = fixture.gql();
    let mut caller = FakeRunner::new(
        fixture.nats(),
        &wire::unique_runner_type(&format!("{label}-c")),
        "instance-c",
    )
    .declaring_capacity(3);
    let mut called = FakeRunner::new(
        fixture.nats(),
        &wire::unique_runner_type(&format!("{label}-y")),
        "instance-y",
    )
    .declaring_capacity(called_capacity);
    caller.connect().await;
    called.connect().await;
    dispatch::await_live_instances(&client, fixture.admin(), &caller.runner_type, 1, LONG).await;
    dispatch::await_live_instances(&client, fixture.admin(), &called.runner_type, 1, LONG).await;
    let root = JobDeclaration::new(&caller.runner_type).with_max_attempts(1);
    Producer::new(fixture.fabric(), "projects")
        .declare_v2(&root)
        .await;
    let trigger = caller.next_trigger(LONG).await;
    caller.start_applied(fixture, &trigger).await;
    let agent = Agent::with_inbox();
    let address = agent.agent_id;
    surface::register(fixture.nats(), &caller, runner::run_id(&trigger), &[agent]).await;
    CallScene {
        work: Work::of(&caller, &trigger),
        caller,
        called,
        root: root.job_id,
        trigger,
        address,
    }
}

impl CallScene<'_> {
    pub async fn call(&self, fixture: &JobsFixture, mode: &str, max_attempts: i64) -> Uuid {
        let job = Uuid::now_v7();
        let mut body = json!({
            "job_id": job.to_string(),
            "runner_type": self.called.runner_type,
            "producer": "projects",
            "parent_job_id": self.root.to_string(),
            "mode": mode,
            "max_attempts": max_attempts,
            "acting_run_id": self.work.run_id.to_string(),
            "acting_instance_key": self.caller.instance_key,
        });
        if mode == "BACKGROUND" {
            body["caller_address"] = json!(self.address.to_string());
        }
        Producer::new(fixture.fabric(), "projects")
            .send_v3(wire::VERB_CREATE, body)
            .await;
        job
    }

    pub async fn resolve(&self, fixture: &JobsFixture, verb: &str, job: Uuid) {
        Producer::new(fixture.fabric(), "projects")
            .send_v3(
                verb,
                json!({
                    "job_id": job.to_string(),
                    "acting_run_id": self.work.run_id.to_string(),
                    "acting_instance_key": self.caller.instance_key,
                }),
            )
            .await;
    }
}
