mod support;

use std::time::Duration;

use serde_json::{Value, json};
use support::db::Durable;
use support::fixture::{JobsFixture, runner_access};
use support::gesture::{self, Work};
use support::producer::{JobDeclaration, Producer};
use support::runner::{self, FakeRunner};
use support::surface::{self, Agent};
use support::tree_hold::TreeHold;
use support::{LONG, dispatch, infra, metrics, wire};

const STARTS_OF_RUN: &str = "SELECT count(*) FROM run_starts WHERE run_id = $1";
const TERMINALS_OF_RUN: &str = "SELECT count(*) FROM run_terminals WHERE run_id = $1";
const BOUNDED_WAIT: Duration = Duration::from_secs(5);

struct Scene<'a> {
    runner: FakeRunner<'a>,
    trigger: Value,
    work: Work,
}

impl Scene<'_> {
    fn tree(&self) -> uuid::Uuid {
        runner::job_id(&self.trigger)
    }

    fn run(&self) -> uuid::Uuid {
        runner::run_id(&self.trigger)
    }
}

async fn dispatched<'a>(fixture: &'a JobsFixture, label: &str) -> Scene<'a> {
    let runner_type = wire::unique_runner_type(label);
    let mut runner = FakeRunner::new(fixture.nats(), &runner_type, "instance-a");
    runner.connect().await;
    dispatch::await_live_instances(&fixture.gql(), fixture.admin(), &runner_type, 1, LONG).await;
    Producer::new(fixture.fabric(), "projects")
        .declare_v2(&JobDeclaration::new(&runner_type))
        .await;
    let trigger = runner.next_trigger(LONG).await;
    let work = Work::of(&runner, &trigger);
    Scene {
        runner,
        trigger,
        work,
    }
}

async fn started<'a>(fixture: &'a JobsFixture, label: &str) -> Scene<'a> {
    let scene = dispatched(fixture, label).await;
    scene.runner.start_applied(fixture, &scene.trigger).await;
    scene
}

fn conflict_reason(response: &Value) -> (&str, &str) {
    (
        response["errors"][0]["extensions"]["code"]
            .as_str()
            .unwrap_or_default(),
        response["errors"][0]["extensions"]["reason"]
            .as_str()
            .unwrap_or_default(),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_runner_gesture_that_cannot_take_the_tree_is_a_conflict_not_a_typed_refusal() {
    let fixture = JobsFixture::start().await;
    let client = fixture.gql();
    let scene = started(&fixture, "lock-timeout-gql").await;
    let timeouts_before = metrics::tree_lock_timeouts(&client).await;
    let hold = TreeHold::take(&fixture.app_url(), scene.tree()).await;

    let waited = tokio::time::Instant::now();
    let response = client
        .query(
            &runner_access(),
            gesture::SAVE_STATE,
            json!({ "input": { "work": scene.work.wire(), "state": { "turn": 1 } } }),
        )
        .await;
    let elapsed = waited.elapsed();

    assert!(
        response["data"].is_null(),
        "a gesture that cannot take the Tree answers no typed outcome: {response}"
    );
    assert_eq!(
        conflict_reason(&response),
        ("CONFLICT", "concurrent_modification"),
        "{response}"
    );
    assert!(
        (BOUNDED_WAIT..LONG).contains(&elapsed),
        "the wait for the Tree is bounded at 5 s, took {elapsed:?}"
    );
    assert!(
        (metrics::tree_lock_timeouts(&client).await - timeouts_before - 1.0).abs() < f64::EPSILON,
        "one timeout is counted, and a timeout is not attempted again"
    );

    hold.release().await;
    gesture::accepted(
        &client
            .query(
                &runner_access(),
                gesture::SAVE_STATE,
                json!({ "input": { "work": scene.work.wire(), "state": { "turn": 2 } } }),
            )
            .await,
        "jobsSaveState",
    );
    fixture.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_run_whose_start_waits_for_the_tree_is_never_told_it_has_not_started() {
    let fixture = JobsFixture::start().await;
    let client = fixture.gql();
    let durable = Durable::open(&fixture.app_url()).await;
    let scene = dispatched(&fixture, "lock-timeout-start").await;
    let hold = TreeHold::take(&fixture.app_url(), scene.tree()).await;
    scene.runner.start_run(&scene.trigger).await;

    let response = gesture::unread(&client, &runner_access(), &scene.work, None).await;

    assert_eq!(
        conflict_reason(&response),
        ("CONFLICT", "concurrent_modification"),
        "a Run whose start fact is acknowledged has started: the gesture waits for it or \
         conflicts, it never answers run_not_started: {response}"
    );
    assert_eq!(
        durable.count(STARTS_OF_RUN, scene.run()).await,
        0,
        "the Tree was held for the whole wait, so the start was not applied"
    );

    hold.release().await;
    durable
        .await_count(STARTS_OF_RUN, scene.run(), 1, "the start of the Run lands")
        .await;
    let after = gesture::unread(&client, &runner_access(), &scene.work, None).await;
    assert_eq!(
        gesture::field(&after, "jobsUnread")["success"],
        true,
        "{after}"
    );
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_surface_request_that_cannot_take_the_tree_gets_no_reply() {
    let fixture = JobsFixture::start().await;
    let client = fixture.gql();
    let scene = started(&fixture, "lock-timeout-nats").await;
    let timeouts_before = metrics::tree_lock_timeouts(&client).await;
    let hold = TreeHold::take(&fixture.app_url(), scene.tree()).await;

    let mut pending = surface::send_without_waiting(
        fixture.nats(),
        format!("jobs.runner.register_agents.{}", scene.runner.runner_type),
        json!({
            "version": 1,
            "run_id": scene.run().to_string(),
            "instance_key": scene.runner.instance_key,
            "agents": [{
                "agent_id": Agent::with_inbox().agent_id.to_string(),
                "has_inbox": true,
                "declares_restart": true,
            }],
        }),
    )
    .await;
    metrics::await_tree_lock_timeouts_above(&client, timeouts_before).await;

    assert!(
        pending.silent_for(Duration::from_secs(1)).await,
        "a request that timed out on the Tree lock is not answered"
    );
    hold.release().await;
    let retried = surface::register(
        fixture.nats(),
        &scene.runner,
        scene.run(),
        &[Agent::with_inbox()],
    )
    .await;
    assert_eq!(retried["accepted"], true, "{retried}");
    fixture.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_runner_fact_that_cannot_take_the_tree_is_redelivered_and_lands_when_it_is_free() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let scene = started(&fixture, "lock-timeout-fact").await;
    let redelivered_before = infra::status_frames_redelivered(fixture.nats()).await;
    let hold = TreeHold::take(&fixture.app_url(), scene.tree()).await;

    scene.runner.complete_run(&scene.trigger).await;
    let redelivered = br_test_harness::wait_until(LONG, || async {
        infra::status_frames_redelivered(fixture.nats()).await > redelivered_before
    })
    .await;
    assert!(
        redelivered,
        "the completion fact was never refused for redelivery while the Tree was held"
    );
    assert_eq!(
        durable.count(TERMINALS_OF_RUN, scene.run()).await,
        0,
        "the Tree was held, so the completion was not applied"
    );

    hold.release().await;
    durable
        .await_count(
            TERMINALS_OF_RUN,
            scene.run(),
            1,
            "the redelivered completion lands once the Tree is free",
        )
        .await;
    durable.close().await;
    fixture.shutdown().await;
}
