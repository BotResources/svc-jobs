mod support;

use serde_json::{Value, json};
use support::db::Durable;
use support::fixture::{JobsFixture, runner_access};
use support::gesture::{self, Work};
use support::restart::{FINISHED_OF_RUN, RESTARTS_OF_RUN, Scene, running_child};
use support::{LONG, gql, runner};

const STATE_OF_RUN: &str = "SELECT state::text FROM saved_states WHERE run_id = $1";

async fn save(fixture: &JobsFixture, work: &Work, state: Value) -> Value {
    fixture
        .gql()
        .query(
            &runner_access(),
            gesture::SAVE_STATE,
            json!({ "input": { "work": work.wire(), "state": state } }),
        )
        .await
}

async fn saved(fixture: &JobsFixture, work: &Work) -> Value {
    let read = fixture
        .gql()
        .query(
            &runner_access(),
            gesture::SAVED_STATE,
            json!({ "work": work.wire() }),
        )
        .await;
    gesture::field(&read, "jobsSavedState").clone()
}

async fn state_of(durable: &Durable, run: uuid::Uuid) -> Value {
    let text: String = sqlx::query_scalar(STATE_OF_RUN)
        .bind(run)
        .fetch_one(durable.pool())
        .await
        .expect("a state is saved");
    serde_json::from_str(&text).expect("the state is JSON")
}

async fn read_everything(fixture: &JobsFixture, scene: &Scene<'_>, work: &Work) {
    for address in [scene.receiver, scene.bystander] {
        let items: Vec<uuid::Uuid> = gesture::items(
            &gesture::unread(&fixture.gql(), &runner_access(), work, Some(address)).await,
        )
        .iter()
        .filter_map(|item| item["itemId"].as_str().and_then(|raw| raw.parse().ok()))
        .collect();
        if !items.is_empty() {
            let marked =
                gesture::mark_read(fixture.nats(), &scene.pair.child, work.run_id, &items).await;
            assert_eq!(marked["accepted"], true, "{marked}");
        }
    }
}

async fn restart(fixture: &JobsFixture, scene: &mut Scene<'_>) -> (Value, Work) {
    scene.mail_receiver(fixture, scene.sender).await;
    let trigger = scene.pair.child.next_trigger(LONG).await;
    scene.pair.child.start_applied(fixture, &trigger).await;
    let work = Work::of(&scene.pair.child, &trigger);
    (trigger, work)
}

#[tokio::test]
async fn the_last_saved_state_counts_within_its_limit_and_a_restart_without_a_new_one_keeps_it() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let mut scene = running_child(&fixture, "state-rules").await;
    gesture::accepted(
        &save(&fixture, &scene.child_work, json!({ "turn": 8 })).await,
        "jobsSaveState",
    );
    assert_eq!(
        state_of(&durable, scene.child_run()).await,
        json!({ "turn": 8 }),
        "the second replaces the first"
    );
    let oversized = save(
        &fixture,
        &scene.child_work,
        json!({ "blob": "x".repeat(2 * 1024 * 1024) }),
    )
    .await;
    assert_eq!(
        gesture::refusal(&oversized, "jobsSaveState"),
        "state_too_large"
    );
    assert_eq!(
        state_of(&durable, scene.child_run()).await,
        json!({ "turn": 8 }),
        "the previous one stays"
    );
    scene.finish_child(&durable).await;
    let after_end = save(&fixture, &scene.child_work, json!({ "turn": 9 })).await;
    assert_eq!(gesture::refusal(&after_end, "jobsSaveState"), "run_ended");

    let not_own = Work {
        job_id: scene.child_work.job_id,
        ..scene.parent_work.clone()
    };
    let read = fixture
        .gql()
        .query(
            &runner_access(),
            gesture::SAVED_STATE,
            json!({ "work": not_own.wire() }),
        )
        .await;
    assert_eq!(gesture::refusal(&read, "jobsSavedState"), "job_not_of_run");

    let (first_restart, first_work) = restart(&fixture, &mut scene).await;
    assert_eq!(
        saved(&fixture, &first_work).await["state"],
        json!({ "turn": 8 })
    );
    for agent in [scene.receiver, scene.bystander] {
        let registered = support::surface::register(
            fixture.nats(),
            &scene.pair.child,
            first_work.run_id,
            &[support::surface::Agent::with_inbox().with_id(agent)],
        )
        .await;
        assert_eq!(registered["accepted"], true, "{registered}");
    }
    read_everything(&fixture, &scene, &first_work).await;
    scene.pair.child.complete_run(&first_restart).await;
    durable
        .await_count(
            FINISHED_OF_RUN,
            first_work.run_id,
            2,
            "the restart Run ends with success",
        )
        .await;
    assert_eq!(
        scene.restartable_now(&fixture, scene.receiver).await,
        true,
        "a restart Run that hands no new state leaves its agents restartable"
    );
    let (_, second_work) = restart(&fixture, &mut scene).await;
    assert_eq!(
        saved(&fixture, &second_work).await["state"],
        json!({ "turn": 8 }),
        "the earlier state is kept"
    );
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_restart_receives_only_its_work_leaves_the_old_job_alone_and_a_later_mail_starts_another()
{
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let mut scene = running_child(&fixture, "restart-work").await;
    scene.finish_child(&durable).await;
    let old_job = scene.child_work.job_id;
    let before = gql::job_view(&fixture.gql(), fixture.admin(), old_job).await;

    let mail = scene.mail_receiver(&fixture, scene.sender).await;
    let trigger = scene.pair.child.next_trigger(LONG).await;
    let text = trigger.to_string();
    for absent in [
        "turn",
        &scene.receiver.to_string(),
        &scene.bystander.to_string(),
        &mail.mail_id.to_string(),
    ] {
        assert!(
            !text.contains(absent),
            "the work does not carry {absent}: {trigger}"
        );
    }
    scene.pair.child.start_applied(&fixture, &trigger).await;
    let work = Work::of(&scene.pair.child, &trigger);
    let state = saved(&fixture, &work).await;
    assert_eq!(
        state["state"],
        json!({ "turn": 7 }),
        "the restart Run reads the saved state"
    );
    assert_eq!(
        state["agentIds"].as_array().map(Vec::len),
        Some(2),
        "and the agent ids"
    );
    let unread = gesture::items(
        &gesture::unread(
            &fixture.gql(),
            &runner_access(),
            &work,
            Some(scene.receiver),
        )
        .await,
    );
    assert_eq!(
        unread[0]["mailId"],
        mail.mail_id.to_string(),
        "and then the unread mail"
    );
    assert_eq!(
        gql::job_view(&fixture.gql(), fixture.admin(), old_job).await,
        before,
        "the old Job is untouched"
    );

    for agent in [scene.receiver, scene.bystander] {
        support::surface::register(
            fixture.nats(),
            &scene.pair.child,
            work.run_id,
            &[support::surface::Agent::with_inbox().with_id(agent)],
        )
        .await;
    }
    read_everything(&fixture, &scene, &work).await;
    scene.pair.child.complete_run(&trigger).await;
    durable
        .await_count(
            FINISHED_OF_RUN,
            work.run_id,
            2,
            "the restart ended with success",
        )
        .await;
    scene.pair.child.expect_no_trigger(support::QUIET).await;
    durable
        .assert_count(
            RESTARTS_OF_RUN,
            work.run_id,
            0,
            "nothing waits unread: no restart yet",
        )
        .await;
    scene.mail_receiver(&fixture, scene.sender).await;
    let again = scene.pair.child.next_trigger(LONG).await;
    assert_ne!(
        runner::job_id(&again),
        runner::job_id(&trigger),
        "one more mail starts a new restart Job"
    );
    durable
        .assert_count(
            RESTARTS_OF_RUN,
            work.run_id,
            1,
            "it restarts the Run of the first restart",
        )
        .await;
    durable.close().await;
    fixture.shutdown().await;
}
