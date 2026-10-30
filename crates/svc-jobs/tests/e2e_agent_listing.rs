mod support;

use serde_json::{Value, json};
use support::db::Durable;
use support::fixture::{JobsFixture, Knobs};
use support::producer::{JobDeclaration, Producer};
use support::restart::{FINISHED_OF_RUN, save_state};
use support::runner::{self, FakeRunner};
use support::surface::{self, Agent};
use support::tree::pair;
use support::{LONG, dispatch, gql, wire};
use uuid::Uuid;

async fn finished_child(
    fixture: &JobsFixture,
    caller_instance: &str,
    worker: &mut FakeRunner<'_>,
    root: Uuid,
    root_run: Uuid,
    saves: bool,
    durable: &Durable,
) -> (Uuid, Uuid) {
    let job = Uuid::now_v7();
    Producer::new(fixture.fabric(), "projects")
        .send_v3(
            wire::VERB_CREATE,
            json!({
                "job_id": job.to_string(),
                "runner_type": worker.runner_type,
                "producer": "projects",
                "parent_job_id": root.to_string(),
                "mode": "SYNC",
                "acting_run_id": root_run.to_string(),
                "acting_instance_key": caller_instance,
            }),
        )
        .await;
    let trigger = worker.next_trigger(LONG).await;
    worker.start_applied(fixture, &trigger).await;
    let agent = Agent::with_inbox();
    let id = agent.agent_id;
    let run = runner::run_id(&trigger);
    surface::register(fixture.nats(), worker, run, &[agent]).await;
    if saves {
        save_state(
            fixture,
            &support::gesture::Work::of(worker, &trigger),
            json!({ "n": 1 }),
        )
        .await;
    }
    worker.complete_run(&trigger).await;
    durable
        .await_count(FINISHED_OF_RUN, run, 1, "the child agent finished")
        .await;
    (id, job)
}

fn entry_of<'a>(agents: &'a [Value], agent: Uuid, key: &str) -> Option<&'a Value> {
    agents
        .iter()
        .find(|listed| listed[key] == agent.to_string())
}

fn state_of(agents: &[Value], agent: Uuid) -> (Value, Value) {
    let entry = entry_of(agents, agent, "agent_id")
        .unwrap_or_else(|| panic!("{agent} is listed: {agents:?}"));
    let restartable = entry.get("restartable_now").cloned().unwrap_or(Value::Null);
    (entry["state"].clone(), restartable)
}

#[tokio::test]
async fn the_agent_list_shows_the_running_and_restartable_agents_of_the_own_tree_only() {
    let fixture = JobsFixture::start_with(Knobs {
        retirement_quiet_period_seconds: 3,
        runner_type_impact_interval_seconds: 1,
        ..Knobs::default()
    })
    .await;
    let durable = Durable::open(&fixture.app_url()).await;
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut pair = pair(&fixture, "agent-list").await;
    let mut retiring = FakeRunner::new(
        fixture.nats(),
        &wire::unique_runner_type("retiring"),
        "instance-r",
    )
    .declaring_capacity(3);
    retiring.connect().await;
    dispatch::await_live_instances(
        &fixture.gql(),
        fixture.admin(),
        &retiring.runner_type,
        1,
        LONG,
    )
    .await;
    let root = JobDeclaration::new(&pair.parent.runner_type).with_max_attempts(1);
    producer.declare_v2(&root).await;
    let root_trigger = pair.parent.next_trigger(LONG).await;
    pair.parent.start_applied(&fixture, &root_trigger).await;
    let g_run = runner::run_id(&root_trigger);
    let g = Agent::with_inbox();
    let g_id = g.agent_id;
    surface::register(fixture.nats(), &pair.parent, g_run, &[g]).await;

    let caller = pair.parent.instance_key.clone();
    let child = &mut pair.child;
    let (h, _) = finished_child(&fixture, &caller, child, root.job_id, g_run, true, &durable).await;
    let (k, _) = finished_child(
        &fixture,
        &caller,
        child,
        root.job_id,
        g_run,
        false,
        &durable,
    )
    .await;
    gql::deprecate_runner_type(&fixture.gql(), fixture.admin(), &retiring.runner_type).await;
    let (l, l_job) = finished_child(
        &fixture,
        &caller,
        &mut retiring,
        root.job_id,
        g_run,
        true,
        &durable,
    )
    .await;
    producer
        .send_v3(
            wire::VERB_FINISH,
            json!({
                "job_id": l_job.to_string(),
                "acting_run_id": g_run.to_string(),
                "acting_instance_key": caller,
            }),
        )
        .await;
    gql::wait_for_status(&fixture.gql(), fixture.admin(), l_job, "COMPLETED", LONG).await;
    let retired = br_test_harness::wait_until(LONG, || async {
        gql::retire_runner_type(&fixture.gql(), fixture.admin(), &retiring.runner_type).await
            ["data"][wire::FIELD_RETIRE_RUNNER_TYPE]["success"]
            == json!(true)
    })
    .await;
    assert!(retired, "L's runner type is retired");

    let other = JobDeclaration::new(&pair.parent.runner_type).with_max_attempts(1);
    producer.declare_v2(&other).await;
    let other_trigger = pair.parent.next_trigger(LONG).await;
    pair.parent.start_applied(&fixture, &other_trigger).await;
    let t = Agent::with_inbox();
    let t_id = t.agent_id;
    surface::register(
        fixture.nats(),
        &pair.parent,
        runner::run_id(&other_trigger),
        &[t],
    )
    .await;

    let listing = surface::list(fixture.nats(), &pair.parent, g_run).await;
    assert_eq!(listing["accepted"], true, "{listing}");
    let agents = listing["agents"].as_array().cloned().unwrap_or_default();
    assert_eq!(state_of(&agents, g_id), (json!("RUNNING"), Value::Null));
    assert_eq!(state_of(&agents, h), (json!("FINISHED"), json!(true)));
    assert_eq!(state_of(&agents, l), (json!("FINISHED"), json!(false)));
    assert!(
        entry_of(&agents, k, "agent_id").is_none(),
        "K is not listed"
    );
    assert!(
        entry_of(&agents, t_id, "agent_id").is_none(),
        "the list never shows another Tree"
    );
    assert_eq!(agents.len(), 3, "{agents:?}");
    assert!(
        agents
            .iter()
            .any(|a| a["runner_type"] == json!(pair.child.runner_type)),
        "agents of other runners of the Tree are listed"
    );

    let not_own = surface::list(fixture.nats(), &pair.child, g_run).await;
    assert_eq!(surface::reason(&not_own), "instance_mismatch");

    durable.close().await;
    fixture.shutdown().await;
}
