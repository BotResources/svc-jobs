mod support;

use serde_json::json;
use support::db::{self, Durable};
use support::directory;
use support::fixture::{JobsFixture, runner_access};
use support::gesture::{self, Mail};
use support::lineage::act;
use support::producer::Producer;
use support::restart::{FINISHED_OF_RUN, RESTARTS_OF_RUN, running_child, save_state};
use support::scrub::{age_tree, await_scrubbed};
use support::scrub_scene::{ENDED_RUN, live_root};
use support::surface::{self, Agent};
use support::{LONG, QUIET, gql, wire};
use uuid::Uuid;

#[tokio::test]
async fn after_a_scrub_no_agent_can_restart_and_no_job_can_be_retried_by_hand() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let mut scrubbed = running_child(&fixture, "refusal-t").await;
    let mut other = running_child(&fixture, "refusal-u").await;
    scrubbed.finish_child(&durable).await;
    other.finish_child(&durable).await;
    assert_eq!(
        scrubbed.restartable_now(&fixture, scrubbed.receiver).await,
        json!(true)
    );

    let mail = Mail::new(other.sender, &[other.receiver]);
    gesture::accepted(
        &gesture::send_mail(&fixture.gql(), &runner_access(), &other.parent_work, &mail).await,
        "jobsSendMail",
    );
    let restart = other.pair.child.next_trigger(LONG).await;
    assert_ne!(
        support::runner::job_id(&restart),
        other.child_work.job_id,
        "a mail to a finished agent of a Tree that is not scrubbed starts a restart"
    );

    act(
        &fixture,
        wire::VERB_FINISH,
        scrubbed.child_work.job_id,
        &scrubbed.parent_work,
    )
    .await;
    gql::wait_for_status(
        &fixture.gql(),
        fixture.admin(),
        scrubbed.child_work.job_id,
        "COMPLETED",
        LONG,
    )
    .await;
    scrubbed
        .pair
        .parent
        .complete_run(&scrubbed.parent_trigger)
        .await;
    durable
        .await_count(ENDED_RUN, scrubbed.parent_run(), 1, "the Run of F ends")
        .await;
    Producer::new(fixture.fabric(), "projects")
        .fail_v2(scrubbed.root, Some("the producer gave up"))
        .await;
    gql::wait_for_status(
        &fixture.gql(),
        fixture.admin(),
        scrubbed.root,
        "FAILED",
        LONG,
    )
    .await;
    let before = gql::job_view(&fixture.gql(), fixture.admin(), scrubbed.root).await;
    gql::assert_allowed(&before, "manual_retry");

    age_tree(&durable, scrubbed.root, 31).await;
    await_scrubbed(&durable, scrubbed.root, "the Tree of F is scrubbed").await;

    let resolution = Uuid::parse_str(
        before["job"]["resolution"]["id"]
            .as_str()
            .expect("F names its resolution"),
    )
    .unwrap();
    let successor = Uuid::now_v7();
    let refused = gql::manual_retry_job(
        &fixture.gql(),
        fixture.admin(),
        Uuid::now_v7(),
        scrubbed.root,
        successor,
        resolution,
    )
    .await;
    assert_eq!(
        gql::mutation_error_reason(&refused, "the retry of a scrubbed Job"),
        "tree_scrubbed",
        "{refused}"
    );
    durable
        .assert_count(db::JOBS_WITH_ID, successor, 0, "no successor is created")
        .await;
    let after = gql::job_view(&fixture.gql(), fixture.admin(), scrubbed.root).await;
    assert_eq!(gql::assert_blocked(&after, "manual_retry"), "tree_scrubbed");
    assert!(after["job"]["manualRetry"].is_null(), "{after}");

    let to_q = Mail::new(other.sender, &[scrubbed.receiver]);
    let sent =
        gesture::send_mail(&fixture.gql(), &runner_access(), &other.parent_work, &to_q).await;
    assert_eq!(gesture::refusal(&sent, "jobsSendMail"), "bad_addresses");
    assert_eq!(
        gesture::field(&sent, "jobsSendMail")["badAddresses"][0]["reason"],
        "other_tree"
    );
    scrubbed.pair.child.expect_no_trigger(QUIET).await;
    durable
        .assert_count(RESTARTS_OF_RUN, scrubbed.child_run(), 0, "no restart of Q")
        .await;
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn the_administrator_view_of_a_scrubbed_tree_shows_no_agent_as_restartable() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let live = live_root(&fixture, "refusal-agent", 1).await;
    let agent = Agent::with_inbox();
    let q = agent.agent_id;
    surface::register(fixture.nats(), &live.runner, live.work.run_id, &[agent]).await;
    save_state(&fixture, &live.work, json!({ "turn": 1 })).await;
    live.runner.complete_run(&live.trigger).await;
    durable
        .await_count(FINISHED_OF_RUN, live.work.run_id, 1, "Q finished")
        .await;
    Producer::new(fixture.fabric(), "projects")
        .fail_v2(live.root, Some("the producer gave up"))
        .await;
    gql::wait_for_status(&fixture.gql(), fixture.admin(), live.root, "FAILED", LONG).await;

    let before =
        directory::listed(&directory::tree_agents(&fixture, fixture.admin(), live.root).await);
    let entry = directory::entry_of(&before, q, "agentId")
        .unwrap_or_else(|| panic!("Q is listed before the scrub: {before:?}"));
    assert_eq!(entry["state"], "FINISHED", "{entry}");
    assert_eq!(entry["restartableNow"], json!(true), "{entry}");
    let view = gql::job_view(&fixture.gql(), fixture.admin(), live.root).await;
    gql::assert_allowed(&view, "manual_retry");

    age_tree(&durable, live.root, 31).await;
    await_scrubbed(&durable, live.root, "the Tree of F is scrubbed").await;

    let after =
        directory::listed(&directory::tree_agents(&fixture, fixture.admin(), live.root).await);
    assert!(
        directory::entry_of(&after, q, "agentId").is_none(),
        "the saved state of Q is erased, so the directory no longer lists it: {after:?}"
    );
    durable.close().await;
    fixture.shutdown().await;
}
