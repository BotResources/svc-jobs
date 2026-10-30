mod support;

use serde_json::json;
use support::fixture::JobsFixture;
use support::producer::{JobDeclaration, Producer};
use support::runner;
use support::runner::FakeRunner;
use support::surface::{self, Agent};
use support::tree::{call_child, pair};
use support::{LONG, dispatch, wire};
use uuid::Uuid;

#[tokio::test]
async fn a_registration_is_accepted_only_for_a_started_run_of_the_sending_instance() {
    let fixture = JobsFixture::start().await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let producer = Producer::new(fixture.fabric(), "projects");
    let runner_type = wire::unique_runner_type("directory-guard");
    let mut instance = FakeRunner::new(fixture.nats(), &runner_type, "instance-a");
    instance.connect().await;
    dispatch::await_live_instances(&client, admin, &runner_type, 1, LONG).await;
    let nats = fixture.nats();

    let unknown = surface::register(nats, &instance, Uuid::now_v7(), &[Agent::with_inbox()]).await;
    assert_eq!(surface::reason(&unknown), "run_unknown");

    let declaration = JobDeclaration::new(&runner_type);
    producer.declare_v2(&declaration).await;
    let trigger = instance.next_trigger(LONG).await;
    let run = runner::run_id(&trigger);
    let before_start = surface::register(nats, &instance, run, &[Agent::with_inbox()]).await;
    assert_eq!(surface::reason(&before_start), "run_not_started");

    instance.start_applied(&fixture, &trigger).await;
    let stranger = surface::register_as(
        nats,
        &runner_type,
        "instance-b",
        run,
        &[Agent::with_inbox()],
    )
    .await;
    assert_eq!(surface::reason(&stranger), "instance_mismatch");

    let generator = Agent::with_inbox();
    let critic = Agent::without_inbox();
    let both = surface::register(nats, &instance, run, &[generator, critic]).await;
    assert_eq!(both["accepted"], true, "{both}");

    let listing = surface::list(nats, &instance, run).await;
    assert_eq!(listing["accepted"], true, "{listing}");
    let agents = listing["agents"].as_array().expect("agents are listed");
    assert_eq!(agents.len(), 2);
    let with_inbox = agents
        .iter()
        .find(|agent| agent["has_inbox"] == true)
        .expect("the generator is listed");
    assert_eq!(with_inbox["state"], "RUNNING");
    assert_eq!(with_inbox["mail_address"], with_inbox["agent_id"]);
    assert_eq!(with_inbox["run_id"], run.to_string());
    let without_inbox = agents
        .iter()
        .find(|agent| agent["has_inbox"] == false)
        .expect("the critic is listed");
    assert!(without_inbox.get("mail_address").is_none());

    let runner_made_id = surface::register(
        nats,
        &instance,
        run,
        &[Agent::with_inbox().with_id(Uuid::new_v4())],
    )
    .await;
    assert_eq!(runner_made_id["accepted"], true, "{runner_made_id}");
    let twice = Uuid::now_v7();
    let same_agent_twice = surface::register(
        nats,
        &instance,
        run,
        &[
            Agent::with_inbox().with_id(twice),
            Agent::with_inbox().with_id(twice),
        ],
    )
    .await;
    assert_eq!(surface::reason(&same_agent_twice), "malformed");
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_repeated_registration_is_absorbed_and_a_later_one_replaces_what_the_agent_declared() {
    let fixture = JobsFixture::start().await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let producer = Producer::new(fixture.fabric(), "projects");
    let runner_type = wire::unique_runner_type("directory-replace");
    let mut instance = FakeRunner::new(fixture.nats(), &runner_type, "instance-a");
    instance.connect().await;
    dispatch::await_live_instances(&client, admin, &runner_type, 1, LONG).await;
    let nats = fixture.nats();
    producer
        .declare_v2(&JobDeclaration::new(&runner_type))
        .await;
    let trigger = instance.next_trigger(LONG).await;
    instance.start_applied(&fixture, &trigger).await;
    let run = runner::run_id(&trigger);

    let agent_id = Uuid::now_v7();
    for _ in 0..2 {
        let reply = surface::register(
            nats,
            &instance,
            run,
            &[Agent::with_inbox().with_id(agent_id)],
        )
        .await;
        assert_eq!(reply["accepted"], true, "{reply}");
    }
    let replaced = surface::register(
        nats,
        &instance,
        run,
        &[Agent::without_inbox().with_id(agent_id)],
    )
    .await;
    assert_eq!(replaced["accepted"], true, "{replaced}");

    let listing = surface::list(nats, &instance, run).await;
    let agents = listing["agents"].as_array().expect("agents are listed");
    assert_eq!(agents.len(), 1, "one entry for each agent id");
    assert_eq!(agents[0]["has_inbox"], false);
    assert!(agents[0].get("mail_address").is_none());
    fixture.shutdown().await;
}

#[tokio::test]
async fn an_agent_of_another_run_is_taken_over_only_when_finished_and_in_the_same_tree() {
    let fixture = JobsFixture::start().await;
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut pair = pair(&fixture, "directory-takeover").await;
    let nats = fixture.nats();
    let root = JobDeclaration::new(&pair.parent.runner_type);
    producer.declare_v2(&root).await;
    let parent_trigger = pair.parent.next_trigger(LONG).await;
    pair.parent.start_applied(&fixture, &parent_trigger).await;
    let parent_run = runner::run_id(&parent_trigger);
    let shared = Agent::with_inbox();
    let shared_id = shared.agent_id;
    assert_eq!(
        surface::register(nats, &pair.parent, parent_run, &[shared]).await["accepted"],
        true
    );

    let called = call_child(&fixture, &mut pair, root.job_id, parent_run).await;
    pair.child.start_applied(&fixture, &called).await;
    let child_run = runner::run_id(&called);
    let while_running = surface::register(
        nats,
        &pair.child,
        child_run,
        &[Agent::with_inbox().with_id(shared_id)],
    )
    .await;
    assert_eq!(surface::reason(&while_running), "agent_id_not_finished");

    pair.parent.complete_run(&parent_trigger).await;
    let taken_over = br_test_harness::wait_until(LONG, || async {
        let attempt = surface::register(
            nats,
            &pair.child,
            child_run,
            &[Agent::with_inbox().with_id(shared_id)],
        )
        .await;
        attempt["accepted"] == true
    })
    .await;
    assert!(
        taken_over,
        "a finished agent of the same tree is taken over"
    );
    let listing = surface::list(nats, &pair.child, child_run).await;
    let entry = listing["agents"]
        .as_array()
        .and_then(|agents| {
            agents
                .iter()
                .find(|a| a["agent_id"] == shared_id.to_string())
        })
        .cloned()
        .expect("the agent keeps its entry");
    assert_eq!(entry["run_id"], child_run.to_string());
    assert_eq!(entry["state"], "RUNNING");

    let other_tree = JobDeclaration::new(&pair.parent.runner_type);
    producer.declare_v2(&other_tree).await;
    let other_trigger = pair.parent.next_trigger(LONG).await;
    pair.parent.start_applied(&fixture, &other_trigger).await;
    let foreign = surface::register(
        nats,
        &pair.parent,
        runner::run_id(&other_trigger),
        &[Agent::with_inbox().with_id(shared_id)],
    )
    .await;
    assert_eq!(surface::reason(&foreign), "agent_id_in_other_tree");
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_request_of_another_wire_version_or_naming_an_unreadable_run_is_malformed() {
    let fixture = JobsFixture::start().await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let runner_type = wire::unique_runner_type("directory-malformed");
    let mut instance = FakeRunner::new(fixture.nats(), &runner_type, "instance-a");
    instance.connect().await;
    dispatch::await_live_instances(&client, admin, &runner_type, 1, LONG).await;
    let nats = fixture.nats();
    let run = Uuid::now_v7().to_string();

    for verb in ["register_agents", "list_agents", "mark_read"] {
        let body = |version: u8, run_id: &str| {
            json!({
                "version": version,
                "run_id": run_id,
                "instance_key": instance.instance_key,
                "agents": [],
                "item_ids": [],
            })
        };
        let other_version = surface::raw(nats, verb, &instance, body(2, &run)).await;
        assert_eq!(surface::reason(&other_version), "malformed", "{verb}");
        let not_a_run_id =
            surface::raw(nats, verb, &instance, body(1, &Uuid::new_v4().to_string())).await;
        assert_eq!(surface::reason(&not_a_run_id), "malformed", "{verb}");
    }
    fixture.shutdown().await;
}
