mod support;

use serde_json::json;
use support::db::Durable;
use support::fixture::{JobsFixture, runner_access};
use support::gesture::{self, Work};
use support::restart::{FINISHED_OF_RUN, RESTARTS_OF_RUN, UNREAD_AT, running_child};
use support::runner;
use support::surface::{self, Agent};
use support::{LONG, QUIET};
use uuid::Uuid;

#[tokio::test]
async fn a_direct_mail_restarts_a_finished_agent_as_a_job_that_keeps_its_parent_requester_and_agent_ids()
 {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let mut scene = running_child(&fixture, "restart-keeps").await;
    scene.finish_child(&durable).await;
    let origin_job = scene.child_work.job_id;

    let mail = scene.mail_receiver(&fixture, scene.sender).await;
    let trigger = scene.pair.child.next_trigger(LONG).await;
    let restart_job = runner::job_id(&trigger);
    let restart_run = runner::run_id(&trigger);
    assert_eq!(
        trigger["restart"]["restart_of_run_id"],
        json!(scene.child_run().to_string())
    );
    assert_ne!(restart_job, origin_job, "a restart is a new Job");

    let (parent, requester, tree, has_passport): (Uuid, Option<String>, Uuid, bool) =
        sqlx::query_as(
            "SELECT parent_job_id::uuid, requester ->> 'Run', \
                    COALESCE(root_job_id, id)::uuid, passport IS NOT NULL \
             FROM jobs WHERE id = $1",
        )
        .bind(restart_job)
        .fetch_one(durable.pool())
        .await
        .expect("the restart Job exists");
    assert_eq!(
        parent, scene.root,
        "the restart hangs where the first Job hung"
    );
    assert_eq!(
        requester,
        Some(scene.parent_run().to_string()),
        "the restart answers to the same Requester"
    );
    assert_eq!(tree, scene.root);
    assert!(has_passport, "every Job has a Passport, a restart included");
    durable
        .assert_count(RESTARTS_OF_RUN, scene.child_run(), 1, "one restart link")
        .await;

    scene.pair.child.start_applied(&fixture, &trigger).await;
    let work = Work::of(&scene.pair.child, &trigger);
    let state = fixture
        .gql()
        .query(
            &runner_access(),
            gesture::SAVED_STATE,
            json!({ "work": work.wire() }),
        )
        .await;
    let saved = gesture::field(&state, "jobsSavedState");
    assert_eq!(
        saved["state"],
        json!({ "turn": 7 }),
        "the restart reads the saved state"
    );
    let mut saved_agents: Vec<String> = saved["agentIds"]
        .as_array()
        .expect("the saved agents are listed")
        .iter()
        .map(|id| id.as_str().unwrap_or_default().to_owned())
        .collect();
    saved_agents.sort();
    let mut expected = vec![scene.receiver.to_string(), scene.bystander.to_string()];
    expected.sort();
    assert_eq!(saved_agents, expected, "a restart keeps the agent ids");

    let waiting = gesture::items(
        &gesture::unread(
            &fixture.gql(),
            &runner_access(),
            &work,
            Some(scene.receiver),
        )
        .await,
    );
    assert!(
        waiting
            .iter()
            .any(|item| item["mailId"] == json!(mail.mail_id.to_string())),
        "the restart Run finds the mail that started it unread: {waiting:?}"
    );

    let registered = surface::register(
        fixture.nats(),
        &scene.pair.child,
        restart_run,
        &[
            Agent::with_inbox().with_id(scene.receiver),
            Agent::with_inbox().with_id(scene.bystander),
        ],
    )
    .await;
    assert_eq!(registered["accepted"], true, "{registered}");
    let listing = surface::list(fixture.nats(), &scene.pair.child, restart_run).await;
    assert!(surface::agent_ids(&listing).contains(&scene.receiver.to_string()));

    scene.pair.child.complete_run(&trigger).await;
    durable
        .await_count(
            FINISHED_OF_RUN,
            restart_run,
            2,
            "the agents finish in the restart Run",
        )
        .await;
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_cc_notice_never_restarts_a_finished_agent() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let mut scene = running_child(&fixture, "restart-cc").await;
    scene.finish_child(&durable).await;

    let mail = gesture::Mail::new(scene.sender, &[scene.peer]).cc(&[scene.bystander]);
    let sent =
        gesture::send_mail(&fixture.gql(), &runner_access(), &scene.parent_work, &mail).await;
    gesture::accepted(&sent, "jobsSendMail");

    scene.pair.child.expect_no_trigger(QUIET).await;
    durable
        .assert_count(RESTARTS_OF_RUN, scene.child_run(), 0, "no restart was made")
        .await;
    durable
        .assert_count(
            UNREAD_AT,
            scene.bystander,
            1,
            "the notice waits unread until the agent next starts",
        )
        .await;
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_failed_restart_run_puts_the_agents_back_to_finished_and_leaves_the_mail_unread() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let mut scene = running_child(&fixture, "restart-fails").await;
    scene.finish_child(&durable).await;
    scene.mail_receiver(&fixture, scene.sender).await;
    let trigger = scene.pair.child.next_trigger(LONG).await;
    scene.pair.child.start_applied(&fixture, &trigger).await;
    surface::register(
        fixture.nats(),
        &scene.pair.child,
        runner::run_id(&trigger),
        &[
            Agent::with_inbox().with_id(scene.receiver),
            Agent::with_inbox().with_id(scene.bystander),
        ],
    )
    .await;

    scene
        .pair
        .child
        .fail_run(&trigger, "PERMANENT", "boom", None)
        .await;

    durable
        .await_count(
            FINISHED_OF_RUN,
            scene.child_run(),
            2,
            "the agents are back to FINISHED in the Run that ended with success",
        )
        .await;
    durable
        .assert_count(UNREAD_AT, scene.receiver, 1, "the mail stays unread")
        .await;
    scene.pair.child.expect_no_trigger(QUIET).await;
    durable.close().await;
    fixture.shutdown().await;
}
