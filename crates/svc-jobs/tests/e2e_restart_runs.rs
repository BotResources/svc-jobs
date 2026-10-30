mod support;

use serde_json::{Value, json};
use support::db::{Durable, SAVED_STATES_OF_RUN};
use support::fixture::{JobsFixture, runner_access};
use support::gesture::{self, Work};
use support::restart::{FINISHED_OF_RUN, RESTARTS_OF_RUN, Scene, running_child, save_state};
use support::runner;
use support::surface::{self, Agent};
use support::{LONG, QUIET};
use uuid::Uuid;

async fn register_agents(fixture: &JobsFixture, scene: &Scene<'_>, run: Uuid) {
    let registered = surface::register(
        fixture.nats(),
        &scene.pair.child,
        run,
        &[
            Agent::with_inbox().with_id(scene.receiver),
            Agent::with_inbox().with_id(scene.bystander),
        ],
    )
    .await;
    assert_eq!(registered["accepted"], true, "{registered}");
}

async fn saved(fixture: &JobsFixture, work: &Work) -> Value {
    let state = fixture
        .gql()
        .query(
            &runner_access(),
            gesture::SAVED_STATE,
            json!({ "work": work.wire() }),
        )
        .await;
    gesture::field(&state, "jobsSavedState").clone()
}

fn sorted_ids(saved: &Value) -> Vec<String> {
    let mut ids: Vec<String> = saved["agentIds"]
        .as_array()
        .expect("the saved agents are listed")
        .iter()
        .map(|id| id.as_str().unwrap_or_default().to_owned())
        .collect();
    ids.sort();
    ids
}

#[tokio::test]
async fn a_restart_run_ending_with_success_replaces_the_saved_state_of_the_run_it_restarted() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let mut scene = running_child(&fixture, "restart-replaces").await;
    scene.finish_child(&durable).await;
    scene.mail_receiver(&fixture, scene.sender).await;
    let first = scene.pair.child.next_trigger(LONG).await;
    scene.pair.child.start_applied(&fixture, &first).await;
    let first_run = runner::run_id(&first);
    register_agents(&fixture, &scene, first_run).await;
    save_state(
        &fixture,
        &Work::of(&scene.pair.child, &first),
        json!({ "turn": 8 }),
    )
    .await;
    durable
        .assert_count(
            SAVED_STATES_OF_RUN,
            scene.child_run(),
            1,
            "while the restart runs, the state of the Run it restarts is still kept",
        )
        .await;

    scene.pair.child.complete_run(&first).await;
    durable
        .await_count(
            FINISHED_OF_RUN,
            first_run,
            2,
            "the agents finish in the restart Run",
        )
        .await;

    durable
        .await_count(
            SAVED_STATES_OF_RUN,
            scene.child_run(),
            0,
            "the state of the restarted Run is replaced, not kept beside the new one",
        )
        .await;
    durable
        .assert_count(
            SAVED_STATES_OF_RUN,
            first_run,
            1,
            "the restart Run holds the one state",
        )
        .await;
    scene.mail_receiver(&fixture, scene.peer).await;
    let second = scene.pair.child.next_trigger(LONG).await;
    scene.pair.child.start_applied(&fixture, &second).await;
    let read = saved(&fixture, &Work::of(&scene.pair.child, &second)).await;
    assert_eq!(
        read["state"],
        json!({ "turn": 8 }),
        "the next restart reads the new state"
    );
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_transient_failure_of_a_restart_run_is_retried_as_a_restart_of_the_same_run_with_the_same_agents()
 {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let mut scene = running_child(&fixture, "restart-transient").await;
    scene.finish_child(&durable).await;
    scene.mail_receiver(&fixture, scene.sender).await;
    let first = scene.pair.child.next_trigger(LONG).await;
    scene.pair.child.start_applied(&fixture, &first).await;
    register_agents(&fixture, &scene, runner::run_id(&first)).await;

    scene
        .pair
        .child
        .fail_run(&first, "TRANSIENT", "flaky", None)
        .await;

    let retry = scene.pair.child.next_trigger(LONG).await;
    assert_eq!(runner::job_id(&retry), runner::job_id(&first));
    assert_eq!(runner::attempt_number(&retry), 2);
    assert_eq!(
        retry["restart"]["restart_of_run_id"],
        json!(scene.child_run().to_string()),
        "the retry restarts the same Run"
    );
    scene.pair.child.start_applied(&fixture, &retry).await;
    let read = saved(&fixture, &Work::of(&scene.pair.child, &retry)).await;
    assert_eq!(read["state"], json!({ "turn": 7 }));
    let mut expected = vec![scene.receiver.to_string(), scene.bystander.to_string()];
    expected.sort();
    assert_eq!(sorted_ids(&read), expected, "the retry keeps the agent ids");
    register_agents(&fixture, &scene, runner::run_id(&retry)).await;
    durable
        .assert_count(
            RESTARTS_OF_RUN,
            scene.child_run(),
            1,
            "a retry of the restart Job is no second restart",
        )
        .await;
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_mail_after_the_permanent_failure_of_a_restart_starts_a_new_restart_job() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let mut scene = running_child(&fixture, "restart-again").await;
    scene.finish_child(&durable).await;
    scene.mail_receiver(&fixture, scene.sender).await;
    let first = scene.pair.child.next_trigger(LONG).await;
    scene.pair.child.start_applied(&fixture, &first).await;
    register_agents(&fixture, &scene, runner::run_id(&first)).await;
    scene
        .pair
        .child
        .fail_run(&first, "PERMANENT", "boom", None)
        .await;
    durable
        .await_count(
            FINISHED_OF_RUN,
            scene.child_run(),
            2,
            "the agents are back to FINISHED",
        )
        .await;
    scene.pair.child.expect_no_trigger(QUIET).await;

    scene.mail_receiver(&fixture, scene.peer).await;

    let second = scene.pair.child.next_trigger(LONG).await;
    assert_ne!(
        runner::job_id(&second),
        runner::job_id(&first),
        "the new mail starts a new restart Job"
    );
    assert_eq!(
        second["restart"]["restart_of_run_id"],
        json!(scene.child_run().to_string())
    );
    durable
        .assert_count(RESTARTS_OF_RUN, scene.child_run(), 2, "two restart links")
        .await;
    durable.close().await;
    fixture.shutdown().await;
}
