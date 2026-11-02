mod support;

use serde_json::{Value, json};
use support::events::{EventLog, ObservedEvent};
use support::fixture::{JobsFixture, Knobs};
use support::gesture::Work;
use support::lineage::{act_as, create, live_runner, started_with_agent};
use support::producer::{JobDeclaration, Producer};
use support::{LONG, QUIET, gql, wire};
use uuid::Uuid;

struct Scene {
    fixture: JobsFixture,
    first: EventLog,
    current: EventLog,
}

impl Scene {
    async fn start() -> Self {
        Self::start_with(Knobs::default()).await
    }

    async fn start_with(knobs: Knobs) -> Self {
        let fixture = JobsFixture::start_with(knobs).await;
        Self {
            first: EventLog::open(fixture.fabric()).await,
            current: EventLog::open_v2(fixture.fabric()).await,
            fixture,
        }
    }

    fn producer(&self, bc: &str) -> Producer<'_> {
        Producer::new(self.fixture.fabric(), bc)
    }

    async fn declared_by(&self, producer: &Producer<'_>) -> Uuid {
        let declaration = JobDeclaration::new(&wire::unique_runner_type("repeated"));
        producer.declare(&declaration).await;
        self.current
            .expect_exactly(wire::FACT_QUEUED, declaration.job_id, 1, LONG)
            .await;
        declaration.job_id
    }

    async fn status(&self, job: Uuid) -> String {
        gql::status_of(&self.fixture.gql(), self.fixture.admin(), job).await
    }

    async fn refusals(&self, job: Uuid, count: usize) -> Vec<String> {
        self.current
            .expect_exactly(wire::FACT_COMMAND_REJECTED, job, count, LONG)
            .await
            .iter()
            .map(|refusal| {
                refusal.payload()["reason_code"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned()
            })
            .collect()
    }

    async fn stop(self) {
        self.first.stop().await;
        self.current.stop().await;
        self.fixture.shutdown().await;
    }
}

fn payload(job: Uuid) -> Value {
    json!({ "job_id": job.to_string() })
}

fn event_ids(events: &[ObservedEvent]) -> Vec<String> {
    let mut ids: Vec<String> = events
        .iter()
        .map(|event| {
            event.envelope["event_id"]
                .as_str()
                .unwrap_or_default()
                .to_owned()
        })
        .collect();
    ids.sort();
    ids.dedup();
    ids
}

fn correlations(events: &[ObservedEvent]) -> Vec<String> {
    events.iter().map(ObservedEvent::correlation_id).collect()
}

#[tokio::test]
async fn a_create_repeated_after_the_dispatch_gets_the_queued_event_again() {
    let scene = Scene::start().await;
    let mut instance = live_runner(&scene.fixture, "repeated-create").await;
    let producer = scene.producer("projects");
    let declaration = JobDeclaration::new(&instance.runner_type);
    let job = declaration.job_id;
    let command = producer.declare(&declaration).await;
    instance.next_trigger(LONG).await;
    scene
        .current
        .expect_exactly(wire::FACT_QUEUED, job, 1, LONG)
        .await;

    let repeat_correlation = Uuid::now_v7();
    producer
        .send_v3_as(
            command.command_id,
            repeat_correlation,
            wire::VERB_CREATE,
            declaration.payload("projects"),
        )
        .await;

    let queued = scene
        .current
        .expect_exactly(wire::FACT_QUEUED, job, 2, LONG)
        .await;
    assert_eq!(
        event_ids(&queued).len(),
        2,
        "the repeat has a fresh event id"
    );
    assert!(
        correlations(&queued).contains(&repeat_correlation.to_string()),
        "the repeat carries the correlation id of the repeat"
    );
    assert_eq!(
        queued[0].payload()["runner_type"],
        queued[1].payload()["runner_type"],
        "the repeat tells what the first told"
    );
    scene
        .first
        .expect_exactly(wire::FACT_QUEUED, job, 1, QUIET)
        .await;
    scene
        .current
        .expect_none(wire::FACT_CREATION_REJECTED, job, QUIET)
        .await;
    instance.expect_no_trigger(QUIET).await;
    assert_eq!(scene.status(job).await, "IN_PROGRESS");
    scene.stop().await;
}

#[tokio::test]
async fn a_creation_refused_for_a_retired_type_is_judged_again_and_accepted_after_the_reactivation()
{
    let scene = Scene::start_with(Knobs {
        retirement_quiet_period_seconds: 3,
        runner_type_impact_interval_seconds: 1,
        ..Knobs::default()
    })
    .await;
    let instance = live_runner(&scene.fixture, "repeated-retired").await;
    let (client, admin) = (scene.fixture.gql(), scene.fixture.admin());
    gql::deprecate_runner_type(&client, admin, &instance.runner_type).await;
    let retired = br_test_harness::wait_until(LONG, || async {
        gql::retire_runner_type(&client, admin, &instance.runner_type).await["data"]
            [wire::FIELD_RETIRE_RUNNER_TYPE]["success"]
            == json!(true)
    })
    .await;
    assert!(retired, "the type is retired once the quiet period ends");
    let producer = scene.producer("projects");
    let declaration = JobDeclaration::new(&instance.runner_type);
    let job = declaration.job_id;
    let command = producer.declare(&declaration).await;
    let refusal = scene
        .first
        .expect_one(wire::FACT_CREATION_REJECTED, job, LONG)
        .await;
    assert_eq!(
        refusal.payload()["reason_code"],
        json!(wire::REASON_RUNNER_TYPE_RETIRED)
    );
    scene
        .current
        .expect_none(wire::FACT_QUEUED, job, QUIET)
        .await;

    let reactivated = gql::reactivate_runner_type(&client, admin, &instance.runner_type).await;
    gql::expect_success(
        &reactivated,
        wire::FIELD_REACTIVATE_RUNNER_TYPE,
        "reactivating",
    );
    producer
        .send_v3_as(
            command.command_id,
            Uuid::now_v7(),
            wire::VERB_CREATE,
            declaration.payload("projects"),
        )
        .await;

    scene
        .current
        .expect_exactly(wire::FACT_QUEUED, job, 1, LONG)
        .await;
    scene
        .first
        .expect_exactly(wire::FACT_CREATION_REJECTED, job, 1, QUIET)
        .await;
    assert_eq!(scene.status(job).await, "IN_PROGRESS");
    scene.stop().await;
}

#[tokio::test]
async fn a_finish_repeated_under_its_command_id_gets_the_completion_again() {
    let scene = Scene::start().await;
    let producer = scene.producer("projects");
    let job = scene.declared_by(&producer).await;
    let command = Uuid::now_v7();

    producer
        .send_v3_as(command, Uuid::now_v7(), wire::VERB_FINISH, payload(job))
        .await;
    scene
        .current
        .expect_exactly(wire::FACT_COMPLETED, job, 1, LONG)
        .await;
    let repeat_correlation = Uuid::now_v7();
    producer
        .send_v3_as(command, repeat_correlation, wire::VERB_FINISH, payload(job))
        .await;

    let completed = scene
        .current
        .expect_exactly(wire::FACT_COMPLETED, job, 2, LONG)
        .await;
    assert_eq!(event_ids(&completed).len(), 2);
    assert!(correlations(&completed).contains(&repeat_correlation.to_string()));
    scene
        .first
        .expect_exactly(wire::FACT_COMPLETED, job, 1, QUIET)
        .await;
    scene
        .current
        .expect_none(wire::FACT_COMMAND_REJECTED, job, QUIET)
        .await;
    assert_eq!(scene.status(job).await, "COMPLETED");

    producer
        .send_v3_as(
            Uuid::now_v7(),
            Uuid::now_v7(),
            wire::VERB_FINISH,
            payload(job),
        )
        .await;
    assert_eq!(scene.refusals(job, 1).await, vec!["job_terminal"]);
    scene.stop().await;
}

#[tokio::test]
async fn a_fail_repeated_under_its_command_id_gets_the_failure_again_with_the_note_of_the_repeat() {
    let scene = Scene::start().await;
    let producer = scene.producer("projects");
    let job = scene.declared_by(&producer).await;
    let command = Uuid::now_v7();
    let body = |note: &str| json!({ "job_id": job.to_string(), "note": note });

    producer
        .send_v3_as(command, Uuid::now_v7(), wire::VERB_FAIL, body("first"))
        .await;
    scene
        .current
        .expect_exactly(wire::FACT_FAILED, job, 1, LONG)
        .await;
    producer
        .send_v3_as(command, Uuid::now_v7(), wire::VERB_FAIL, body("again"))
        .await;

    let failed = scene
        .current
        .expect_exactly(wire::FACT_FAILED, job, 2, LONG)
        .await;
    let mut notes: Vec<String> = failed
        .iter()
        .map(|event| {
            event.payload()["note"]
                .as_str()
                .unwrap_or_default()
                .to_owned()
        })
        .collect();
    notes.sort();
    assert_eq!(notes, vec!["again", "first"]);
    for event in &failed {
        assert_eq!(event.payload()["failure_cause"], json!("DECLARED_BY_OWNER"));
    }
    scene
        .first
        .expect_exactly(wire::FACT_FAILED, job, 1, QUIET)
        .await;
    scene
        .current
        .expect_none(wire::FACT_COMMAND_REJECTED, job, QUIET)
        .await;
    assert_eq!(scene.status(job).await, "FAILED");
    scene.stop().await;
}

#[tokio::test]
async fn a_cancel_repeated_under_its_command_id_gets_the_acceptance_again() {
    let scene = Scene::start().await;
    let producer = scene.producer("projects");
    let job = scene.declared_by(&producer).await;
    let command = Uuid::now_v7();

    producer
        .send_v3_as(command, Uuid::now_v7(), wire::VERB_CANCEL, payload(job))
        .await;
    scene
        .current
        .expect_exactly(wire::FACT_CANCEL_ACCEPTED, job, 1, LONG)
        .await;
    scene
        .current
        .expect_exactly(wire::FACT_CANCELLED, job, 1, LONG)
        .await;
    producer
        .send_v3_as(command, Uuid::now_v7(), wire::VERB_CANCEL, payload(job))
        .await;

    let accepted = scene
        .current
        .expect_exactly(wire::FACT_CANCEL_ACCEPTED, job, 2, LONG)
        .await;
    assert_eq!(event_ids(&accepted).len(), 2);
    scene
        .current
        .expect_exactly(wire::FACT_CANCELLED, job, 1, QUIET)
        .await;
    scene
        .first
        .expect_exactly(wire::FACT_CANCELLED, job, 1, QUIET)
        .await;
    scene
        .current
        .expect_none(wire::FACT_COMMAND_REJECTED, job, QUIET)
        .await;
    assert_eq!(scene.status(job).await, "CANCELLED");
    scene.stop().await;
}

#[tokio::test]
async fn only_an_actor_that_may_send_the_command_gets_the_first_outcome_and_a_refusal_is_judged_again()
 {
    let scene = Scene::start().await;
    let owner = scene.producer("projects");
    let stranger = scene.producer("billing");
    let job = scene.declared_by(&owner).await;
    let command = Uuid::now_v7();

    stranger
        .send_v3_as(command, Uuid::now_v7(), wire::VERB_FINISH, payload(job))
        .await;
    assert_eq!(scene.refusals(job, 1).await, vec!["not_requester"]);
    assert_eq!(scene.status(job).await, "PENDING");

    owner
        .send_v3_as(command, Uuid::now_v7(), wire::VERB_FINISH, payload(job))
        .await;
    scene
        .current
        .expect_exactly(wire::FACT_COMPLETED, job, 1, LONG)
        .await;

    stranger
        .send_v3_as(command, Uuid::now_v7(), wire::VERB_FINISH, payload(job))
        .await;
    assert_eq!(
        scene.refusals(job, 2).await,
        vec!["not_requester", "not_requester"]
    );
    scene
        .current
        .expect_exactly(wire::FACT_COMPLETED, job, 1, QUIET)
        .await;
    stranger
        .send_v3_as(command, Uuid::now_v7(), wire::VERB_CANCEL, payload(job))
        .await;
    scene
        .current
        .expect_none(wire::FACT_CANCEL_ACCEPTED, job, QUIET)
        .await;
    scene.stop().await;
}

#[tokio::test]
async fn a_burst_of_the_same_command_is_answered_for_each_delivery_and_refused_for_none() {
    let scene = Scene::start().await;
    let producer = scene.producer("projects");
    let job = scene.declared_by(&producer).await;
    let command = Uuid::now_v7();

    for _ in 0..4 {
        producer
            .send_v3_as(command, Uuid::now_v7(), wire::VERB_FINISH, payload(job))
            .await;
    }

    scene
        .current
        .expect_exactly(wire::FACT_COMPLETED, job, 4, LONG)
        .await;
    scene
        .first
        .expect_exactly(wire::FACT_COMPLETED, job, 1, QUIET)
        .await;
    scene
        .current
        .expect_none(wire::FACT_COMMAND_REJECTED, job, QUIET)
        .await;
    assert_eq!(scene.status(job).await, "COMPLETED");
    scene.stop().await;
}

#[tokio::test]
async fn a_run_repeating_its_finish_of_a_called_job_gets_the_completion_again() {
    let scene = Scene::start().await;
    let (mut caller, mut called) = (
        live_runner(&scene.fixture, "repeated-caller").await,
        live_runner(&scene.fixture, "repeated-called").await,
    );
    let root = JobDeclaration::new(&caller.runner_type);
    Producer::new(scene.fixture.fabric(), "projects")
        .declare(&root)
        .await;
    let (trigger, _) = started_with_agent(&scene.fixture, &mut caller).await;
    let caller_work = Work::of(&caller, &trigger);
    let child = create(
        &scene.fixture,
        &caller_work,
        &called.runner_type,
        root.job_id,
        json!({ "mode": "SYNC" }),
    )
    .await;
    called.next_trigger(LONG).await;
    let command = Uuid::now_v7();

    act_as(
        &scene.fixture,
        command,
        wire::VERB_FINISH,
        child,
        &caller_work,
    )
    .await;
    scene
        .current
        .expect_exactly(wire::FACT_COMPLETED, child, 1, LONG)
        .await;
    act_as(
        &scene.fixture,
        command,
        wire::VERB_FINISH,
        child,
        &caller_work,
    )
    .await;

    scene
        .current
        .expect_exactly(wire::FACT_COMPLETED, child, 2, LONG)
        .await;
    scene
        .current
        .expect_none(wire::FACT_COMMAND_REJECTED, child, QUIET)
        .await;
    assert_eq!(scene.status(child).await, "COMPLETED");
    scene.stop().await;
}

#[tokio::test]
async fn a_create_repeated_after_the_erasure_of_its_request_data_compares_the_skeleton_only() {
    let scene = Scene::start().await;
    let durable = support::db::Durable::open(&scene.fixture.app_url()).await;
    let producer = scene.producer("projects");
    let declaration = JobDeclaration::new(&wire::unique_runner_type("repeated-erased"))
        .with_config(json!({ "seed": 1 }));
    let job = declaration.job_id;
    let command = producer.declare(&declaration).await;
    scene
        .current
        .expect_exactly(wire::FACT_QUEUED, job, 1, LONG)
        .await;
    producer.send_v3(wire::VERB_CANCEL, payload(job)).await;
    scene
        .current
        .expect_exactly(wire::FACT_CANCELLED, job, 1, LONG)
        .await;
    durable
        .assert_count(
            support::erasure::ERASED_STAMP,
            job,
            1,
            "the request data went with the cancel",
        )
        .await;

    producer
        .send_v3_as(
            command.command_id,
            Uuid::now_v7(),
            wire::VERB_CREATE,
            declaration.payload("projects"),
        )
        .await;
    scene
        .current
        .expect_exactly(wire::FACT_QUEUED, job, 2, LONG)
        .await;
    scene
        .first
        .expect_none(wire::FACT_CREATION_REJECTED, job, QUIET)
        .await;

    let another = JobDeclaration::new(&declaration.runner_type)
        .with_id(job)
        .with_max_attempts(2);
    producer.declare(&another).await;
    let refusal = scene
        .first
        .expect_one(wire::FACT_CREATION_REJECTED, job, LONG)
        .await;
    assert_eq!(
        refusal.payload()["reason_code"],
        json!(wire::REASON_ID_REUSE)
    );
    durable.close().await;
    scene.stop().await;
}
