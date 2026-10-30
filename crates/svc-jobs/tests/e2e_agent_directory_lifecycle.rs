mod support;

use support::db::Durable;
use support::fixture::JobsFixture;
use support::producer::{JobDeclaration, Producer};
use support::runner::{self, FakeRunner};
use support::surface::{self, Agent};
use support::tree::{call_child, pair};
use support::{LONG, dispatch, wire};

const IN_TREE: &str = "SELECT count(*) FROM directory_agents WHERE initial_job_id = $1";
const FINISHED_IN_TREE: &str = "SELECT count(*) FROM directory_agents \
                                WHERE initial_job_id = $1 AND state = 'FINISHED'";

#[tokio::test]
async fn a_failed_run_removes_every_descendant_run_whether_it_ended_with_success_or_still_runs() {
    let fixture = JobsFixture::start().await;
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut pair = pair(&fixture, "directory-removal").await;
    let nats = fixture.nats();
    let durable = Durable::open(&fixture.app_url()).await;
    let root = JobDeclaration::new(&pair.parent.runner_type).with_max_attempts(1);
    producer.declare_v2(&root).await;
    let parent_trigger = pair.parent.next_trigger(LONG).await;
    pair.parent.start_applied(&fixture, &parent_trigger).await;
    let parent_run = runner::run_id(&parent_trigger);
    surface::register(nats, &pair.parent, parent_run, &[Agent::with_inbox()]).await;

    let succeeded = call_child(&fixture, &mut pair, root.job_id, parent_run).await;
    pair.child.start_applied(&fixture, &succeeded).await;
    let succeeded_run = runner::run_id(&succeeded);
    surface::register(nats, &pair.child, succeeded_run, &[Agent::with_inbox()]).await;
    pair.child.complete_run(&succeeded).await;
    durable
        .await_count(
            FINISHED_IN_TREE,
            root.job_id,
            1,
            "the successful child agent",
        )
        .await;

    let running = call_child(&fixture, &mut pair, root.job_id, parent_run).await;
    pair.child.start_applied(&fixture, &running).await;
    let running_run = runner::run_id(&running);
    let running_agent = Agent::with_inbox();
    let running_agent_id = running_agent.agent_id;
    surface::register(nats, &pair.child, running_run, &[running_agent]).await;
    assert_eq!(
        surface::agent_ids(&surface::list(nats, &pair.child, running_run).await).len(),
        2,
        "the directory lists the running agents of the whole tree, and not a finished agent that cannot restart"
    );

    pair.parent
        .fail_run(&parent_trigger, "PERMANENT", "provider_refused", None)
        .await;

    durable
        .await_count(
            IN_TREE,
            root.job_id,
            0,
            "a failed run takes its dependents with it",
        )
        .await;
    let late = surface::register(
        nats,
        &pair.child,
        running_run,
        &[Agent::with_inbox().with_id(running_agent_id)],
    )
    .await;
    assert_eq!(surface::reason(&late), "run_removed");

    pair.child.complete_run(&running).await;
    durable
        .assert_count(
            IN_TREE,
            root.job_id,
            0,
            "a later fact does not bring the run back",
        )
        .await;
    let after_end = surface::register(
        nats,
        &pair.child,
        running_run,
        &[Agent::with_inbox().with_id(running_agent_id)],
    )
    .await;
    assert_eq!(surface::reason(&after_end), "run_removed");
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_job_created_while_its_parent_had_no_live_run_belongs_to_no_run_of_that_parent() {
    let fixture = JobsFixture::start().await;
    let producer = Producer::new(fixture.fabric(), "projects");
    let client = fixture.gql();
    let admin = fixture.admin();
    let mut pair = pair(&fixture, "directory-orphan").await;
    let nats = fixture.nats();
    let durable = Durable::open(&fixture.app_url()).await;
    let mut late = FakeRunner::new(
        nats,
        &wire::unique_runner_type("directory-orphan-late"),
        "instance-a",
    );
    let root = JobDeclaration::new(&late.runner_type).with_max_attempts(1);
    producer.declare_v2(&root).await;
    let child = JobDeclaration::new(&pair.child.runner_type).with_parent(root.job_id);
    producer.declare_v2(&child).await;
    let child_trigger = pair.child.next_trigger(LONG).await;
    pair.child.start_applied(&fixture, &child_trigger).await;
    let child_run = runner::run_id(&child_trigger);
    surface::register(nats, &pair.child, child_run, &[Agent::with_inbox()]).await;

    late.connect().await;
    dispatch::await_live_instances(&client, admin, &late.runner_type, 1, LONG).await;
    let root_trigger = late.next_trigger(LONG).await;
    late.start_applied(&fixture, &root_trigger).await;
    surface::register(
        nats,
        &late,
        runner::run_id(&root_trigger),
        &[Agent::with_inbox()],
    )
    .await;
    durable
        .await_count(IN_TREE, root.job_id, 2, "both runs registered an agent")
        .await;

    late.fail_run(&root_trigger, "PERMANENT", "provider_refused", None)
        .await;

    durable
        .await_count(
            IN_TREE,
            root.job_id,
            1,
            "only the agent of the failed run leaves",
        )
        .await;
    let survivor = surface::register(nats, &pair.child, child_run, &[Agent::with_inbox()]).await;
    assert_eq!(survivor["accepted"], true, "{survivor}");
    durable.close().await;
    fixture.shutdown().await;
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
    pair.parent.start_applied(&fixture, &trigger).await;
    let run = runner::run_id(&trigger);
    surface::register(nats, &pair.parent, run, &[Agent::with_inbox()]).await;
    let durable = Durable::open(&fixture.app_url()).await;

    pair.parent.complete_run(&trigger).await;
    durable
        .await_count(
            FINISHED_IN_TREE,
            root.job_id,
            1,
            "the agent of a successful run finishes",
        )
        .await;
    durable
        .assert_count(IN_TREE, root.job_id, 1, "the run stays in the directory")
        .await;
    let ended = surface::register(nats, &pair.parent, run, &[Agent::with_inbox()]).await;
    assert_eq!(surface::reason(&ended), "run_ended");

    producer.cancel(root.job_id).await;
    durable
        .await_count(
            IN_TREE,
            root.job_id,
            0,
            "the cancel of a job removes the agents of its runs, those that ended with success included",
        )
        .await;
    durable.close().await;
    fixture.shutdown().await;
}
