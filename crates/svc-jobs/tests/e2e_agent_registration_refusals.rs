mod support;

use support::LONG;
use support::db::Durable;
use support::fixture::JobsFixture;
use support::producer::{JobDeclaration, Producer};
use support::restart::FINISHED_OF_RUN;
use support::runner;
use support::surface::{self, Agent};
use support::tree::{call_child, pair};

const ENTRIES_OF_AGENT: &str = "SELECT count(*) FROM directory_agents WHERE agent_id = $1";

#[tokio::test]
async fn a_refused_registration_registers_nothing_and_an_ended_run_registers_no_agent() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut pair = pair(&fixture, "registration-refusals").await;
    let nats = fixture.nats();
    let root = JobDeclaration::new(&pair.parent.runner_type).with_max_attempts(1);
    producer.declare_v2(&root).await;
    let root_trigger = pair.parent.next_trigger(LONG).await;
    pair.parent.start_applied(&fixture, &root_trigger).await;
    let root_run = runner::run_id(&root_trigger);
    let shared = Agent::with_inbox();
    let shared_id = shared.agent_id;
    surface::register(nats, &pair.parent, root_run, &[shared]).await;
    let child = call_child(&fixture, &mut pair, root.job_id, root_run).await;
    pair.child.start_applied(&fixture, &child).await;
    let child_run = runner::run_id(&child);

    let fresh = Agent::with_inbox();
    let fresh_id = fresh.agent_id;
    let refused = surface::register(
        nats,
        &pair.child,
        child_run,
        &[fresh, Agent::with_inbox().with_id(shared_id)],
    )
    .await;
    assert_eq!(surface::reason(&refused), "agent_id_not_finished");
    durable
        .assert_count(
            ENTRIES_OF_AGENT,
            fresh_id,
            0,
            "the refused registration registers nothing",
        )
        .await;
    assert!(
        !surface::agent_ids(&surface::list(nats, &pair.child, child_run).await)
            .contains(&fresh_id.to_string()),
        "the other agent of the refused registration is not listed"
    );

    pair.parent.complete_run(&root_trigger).await;
    durable
        .await_count(FINISHED_OF_RUN, root_run, 1, "the shared agent finished")
        .await;
    let ended = surface::register(nats, &pair.parent, root_run, &[Agent::with_inbox()]).await;
    assert_eq!(surface::reason(&ended), "run_ended");

    let other = JobDeclaration::new(&pair.parent.runner_type).with_max_attempts(1);
    producer.declare_v2(&other).await;
    let other_trigger = pair.parent.next_trigger(LONG).await;
    pair.parent.start_applied(&fixture, &other_trigger).await;
    let stray = Agent::with_inbox();
    let stray_id = stray.agent_id;
    let foreign = surface::register(
        nats,
        &pair.parent,
        runner::run_id(&other_trigger),
        &[stray, Agent::with_inbox().with_id(shared_id)],
    )
    .await;
    assert_eq!(surface::reason(&foreign), "agent_id_in_other_tree");
    durable
        .assert_count(
            ENTRIES_OF_AGENT,
            stray_id,
            0,
            "a registration refused for another Tree registers nothing",
        )
        .await;
    durable
        .assert_count(
            "SELECT count(*) FROM directory_agents WHERE agent_id = $1 AND state = 'FINISHED'",
            shared_id,
            1,
            "the entry of the other Tree is untouched",
        )
        .await;
    durable.close().await;
    fixture.shutdown().await;
}
