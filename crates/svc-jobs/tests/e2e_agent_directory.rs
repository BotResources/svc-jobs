mod support;

use serde_json::json;
use support::dispatch;
use support::events::EventLog;
use support::fixture::JobsFixture;
use support::producer::{JobDeclaration, Producer};
use support::runner::{self, FakeRunner};
use support::surface::{self, Agent};
use support::{LONG, wire};
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

    surface::start(&fixture, &instance, &trigger).await;
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
    surface::start(&fixture, &instance, &trigger).await;
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

struct Pair<'a> {
    parent: FakeRunner<'a>,
    child: FakeRunner<'a>,
}

async fn pair<'a>(fixture: &'a JobsFixture, label: &str) -> Pair<'a> {
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
    );
    parent.connect().await;
    child.connect().await;
    dispatch::await_live_instances(&client, admin, &parent.runner_type, 1, LONG).await;
    dispatch::await_live_instances(&client, admin, &child.runner_type, 1, LONG).await;
    Pair { parent, child }
}

async fn call_child(
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

#[tokio::test]
async fn an_agent_of_another_run_is_taken_over_only_when_finished_and_in_the_same_tree() {
    let fixture = JobsFixture::start().await;
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut pair = pair(&fixture, "directory-takeover").await;
    let nats = fixture.nats();
    let root = JobDeclaration::new(&pair.parent.runner_type);
    producer.declare_v2(&root).await;
    let parent_trigger = pair.parent.next_trigger(LONG).await;
    surface::start(&fixture, &pair.parent, &parent_trigger).await;
    let parent_run = runner::run_id(&parent_trigger);
    let shared = Agent::with_inbox();
    let shared_id = shared.agent_id;
    assert_eq!(
        surface::register(nats, &pair.parent, parent_run, &[shared]).await["accepted"],
        true
    );

    let called = call_child(&fixture, &mut pair, root.job_id, parent_run).await;
    surface::start(&fixture, &pair.child, &called).await;
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
    let mut refused = "agent_id_not_finished".to_owned();
    for _ in 0..40 {
        let attempt = surface::register(
            nats,
            &pair.child,
            child_run,
            &[Agent::with_inbox().with_id(shared_id)],
        )
        .await;
        if attempt["accepted"] == true {
            refused.clear();
            break;
        }
        refused = surface::reason(&attempt).to_owned();
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    assert_eq!(
        refused, "",
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
    surface::start(&fixture, &pair.parent, &other_trigger).await;
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
async fn a_failed_run_removes_its_agents_and_those_of_its_descendant_runs_for_good() {
    let fixture = JobsFixture::start().await;
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut pair = pair(&fixture, "directory-removal").await;
    let nats = fixture.nats();
    let root = JobDeclaration::new(&pair.parent.runner_type).with_max_attempts(1);
    producer.declare_v2(&root).await;
    let parent_trigger = pair.parent.next_trigger(LONG).await;
    surface::start(&fixture, &pair.parent, &parent_trigger).await;
    let parent_run = runner::run_id(&parent_trigger);
    let parent_agent = Agent::with_inbox();
    surface::register(nats, &pair.parent, parent_run, &[parent_agent]).await;
    let called = call_child(&fixture, &mut pair, root.job_id, parent_run).await;
    surface::start(&fixture, &pair.child, &called).await;
    let child_run = runner::run_id(&called);
    let child_agent = Agent::with_inbox();
    let child_agent_id = child_agent.agent_id;
    surface::register(nats, &pair.child, child_run, &[child_agent]).await;
    assert_eq!(
        surface::agent_ids(&surface::list(nats, &pair.child, child_run).await).len(),
        2,
        "the directory lists the agents of the whole tree"
    );

    pair.parent
        .fail_run(&parent_trigger, "PERMANENT", "provider_refused", None)
        .await;

    let durable = support::db::Durable::open(&fixture.app_url()).await;
    let mut remaining = 1;
    for _ in 0..80 {
        remaining = durable
            .count(
                "SELECT count(*) FROM directory_agents WHERE initial_job_id = $1",
                root.job_id,
            )
            .await;
        if remaining == 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    assert_eq!(remaining, 0, "a failed run takes its dependents with it");
    let late = surface::register(
        nats,
        &pair.child,
        child_run,
        &[Agent::with_inbox().with_id(child_agent_id)],
    )
    .await;
    assert!(
        ["run_removed", "run_not_started"].contains(&surface::reason(&late)),
        "a removed run registers nothing more: {late}"
    );
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_background_call_must_name_a_mail_address_of_the_calling_run() {
    let fixture = JobsFixture::start().await;
    let answers = EventLog::open_v2(fixture.fabric()).await;
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut pair = pair(&fixture, "directory-caller").await;
    let nats = fixture.nats();
    let first = JobDeclaration::new(&pair.parent.runner_type);
    let second = JobDeclaration::new(&pair.parent.runner_type);
    producer.declare_v2(&first).await;
    producer.declare_v2(&second).await;
    let first_trigger = pair.parent.next_trigger(LONG).await;
    let second_trigger = pair.parent.next_trigger(LONG).await;
    let (first_job, first_run) = (
        runner::job_id(&first_trigger),
        runner::run_id(&first_trigger),
    );
    let (_, second_run) = (
        runner::job_id(&second_trigger),
        runner::run_id(&second_trigger),
    );
    surface::start(&fixture, &pair.parent, &first_trigger).await;
    surface::start(&fixture, &pair.parent, &second_trigger).await;
    let own = Agent::with_inbox();
    let own_address = own.agent_id;
    let foreign = Agent::with_inbox();
    let foreign_address = foreign.agent_id;
    surface::register(nats, &pair.parent, first_run, &[own]).await;
    surface::register(nats, &pair.parent, second_run, &[foreign]).await;

    let call = |address: Uuid| {
        let job_id = Uuid::now_v7();
        (
            job_id,
            json!({
                "job_id": job_id.to_string(),
                "runner_type": pair.child.runner_type,
                "producer": "projects",
                "parent_job_id": first_job.to_string(),
                "mode": "BACKGROUND",
                "caller_address": address.to_string(),
                "acting_run_id": first_run.to_string(),
                "acting_instance_key": pair.parent.instance_key,
            }),
        )
    };
    let (unheld_job, unheld) = call(Uuid::now_v7());
    producer.send_v3(wire::VERB_CREATE, unheld).await;
    let refused = answers
        .expect_one(wire::FACT_CREATION_REJECTED, unheld_job, LONG)
        .await;
    assert_eq!(refused.payload()["reason_code"], "caller_address_not_held");

    let (foreign_job, other_run) = call(foreign_address);
    producer.send_v3(wire::VERB_CREATE, other_run).await;
    let refused = answers
        .expect_one(wire::FACT_CREATION_REJECTED, foreign_job, LONG)
        .await;
    assert_eq!(
        refused.payload()["reason_code"],
        "caller_address_not_of_run"
    );

    let (accepted_job, accepted) = call(own_address);
    producer.send_v3(wire::VERB_CREATE, accepted).await;
    let trigger = pair.child.next_trigger(LONG).await;
    assert_eq!(runner::job_id(&trigger), accepted_job);
    assert_eq!(trigger["mode"], "BACKGROUND");
    assert_eq!(trigger["caller_address"], own_address.to_string());
    answers.stop().await;
    fixture.shutdown().await;
}

async fn eventually_count(
    durable: &support::db::Durable,
    sql: &str,
    id: Uuid,
    expected: i64,
) -> i64 {
    let mut found = -1;
    for _ in 0..80 {
        found = durable.count(sql, id).await;
        if found == expected {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    found
}

#[tokio::test]
async fn a_run_that_ends_with_success_leaves_its_agents_finished_until_its_job_is_cancelled() {
    let fixture = JobsFixture::start().await;
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut pair = pair(&fixture, "directory-finish").await;
    let nats = fixture.nats();
    let root = JobDeclaration::new(&pair.parent.runner_type);
    producer.declare_v2(&root).await;
    let trigger = pair.parent.next_trigger(LONG).await;
    surface::start(&fixture, &pair.parent, &trigger).await;
    let run = runner::run_id(&trigger);
    let agent = Agent::with_inbox();
    surface::register(nats, &pair.parent, run, &[agent]).await;
    let durable = support::db::Durable::open(&fixture.app_url()).await;
    let in_tree = "SELECT count(*) FROM directory_agents WHERE initial_job_id = $1";
    let finished = "SELECT count(*) FROM directory_agents \
                    WHERE initial_job_id = $1 AND state = 'FINISHED'";

    pair.parent.complete_run(&trigger).await;
    assert_eq!(
        eventually_count(&durable, finished, root.job_id, 1).await,
        1
    );
    assert_eq!(durable.count(in_tree, root.job_id).await, 1);

    producer.cancel(root.job_id).await;
    assert_eq!(
        eventually_count(&durable, in_tree, root.job_id, 0).await,
        0,
        "the cancel of a job removes the agents of its runs, those that ended with success included"
    );
    durable.close().await;
    fixture.shutdown().await;
}
