mod support;

use serde_json::{Value, json};
use support::gesture::Work;
use support::lineage::{live_runner, started_with_agent};
use support::producer::{JobDeclaration, Producer};
use support::repeat_scene::Scene;
use support::{LONG, QUIET, wire};
use uuid::Uuid;

struct Calling {
    scene: Scene,
    root: Uuid,
    work: Work,
    address: Uuid,
    called_type: String,
}

impl Calling {
    async fn start(label: &str) -> Self {
        let scene = Scene::start().await;
        let mut caller = live_runner(&scene.fixture, &format!("{label}-caller")).await;
        let called = live_runner(&scene.fixture, &format!("{label}-called")).await;
        let root = JobDeclaration::new(&caller.runner_type);
        Producer::new(scene.fixture.fabric(), "projects")
            .declare(&root)
            .await;
        let (trigger, address) = started_with_agent(&scene.fixture, &mut caller).await;
        Self {
            work: Work::of(&caller, &trigger),
            address,
            root: root.job_id,
            called_type: called.runner_type.clone(),
            scene,
        }
    }

    fn call(&self, job: Uuid, mode: Value) -> Value {
        let mut body = json!({
            "job_id": job.to_string(),
            "runner_type": self.called_type,
            "producer": "projects",
            "parent_job_id": self.root.to_string(),
            "acting_run_id": self.work.run_id.to_string(),
            "acting_instance_key": self.work.instance_key,
        });
        body.as_object_mut()
            .unwrap()
            .extend(mode.as_object().unwrap().clone());
        body
    }

    fn sync(&self, job: Uuid) -> Value {
        self.call(job, json!({ "mode": "SYNC" }))
    }

    fn background(&self, job: Uuid) -> Value {
        self.call(
            job,
            json!({ "mode": "BACKGROUND", "caller_address": self.address.to_string() }),
        )
    }

    async fn send(&self, command: Uuid, body: Value) {
        self.scene
            .producer("projects")
            .send_v3_as(command, Uuid::now_v7(), wire::VERB_CREATE, body)
            .await;
    }
}

#[tokio::test]
async fn a_create_by_a_run_repeated_identically_gets_the_queued_event_again() {
    let calling = Calling::start("repeated-run-create").await;
    let (job, command) = (Uuid::now_v7(), Uuid::now_v7());
    calling.send(command, calling.sync(job)).await;
    calling
        .scene
        .current
        .expect_exactly(wire::FACT_QUEUED, job, 1, LONG)
        .await;

    calling.send(command, calling.sync(job)).await;

    let queued = calling
        .scene
        .current
        .expect_exactly(wire::FACT_QUEUED, job, 2, LONG)
        .await;
    for event in &queued {
        assert_eq!(
            event.payload()["parent_job_id"],
            json!(calling.root.to_string())
        );
        assert_eq!(event.payload()["requester"]["kind"], "RUN");
    }
    calling
        .scene
        .first
        .expect_none(wire::FACT_CREATION_REJECTED, job, QUIET)
        .await;
    calling.scene.stop().await;
}

#[tokio::test]
async fn a_create_by_a_run_that_declares_another_call_is_a_reuse_of_the_id_and_not_a_repeat() {
    let calling = Calling::start("repeated-run-call").await;
    let (job, command) = (Uuid::now_v7(), Uuid::now_v7());
    calling.send(command, calling.sync(job)).await;
    calling
        .scene
        .current
        .expect_exactly(wire::FACT_QUEUED, job, 1, LONG)
        .await;

    calling.send(command, calling.background(job)).await;

    let refusal = calling
        .scene
        .first
        .expect_one(wire::FACT_CREATION_REJECTED, job, LONG)
        .await;
    assert_eq!(
        refusal.payload()["reason_code"],
        json!(wire::REASON_ID_REUSE)
    );
    calling
        .scene
        .current
        .expect_exactly(wire::FACT_QUEUED, job, 1, QUIET)
        .await;
    calling.scene.stop().await;
}
