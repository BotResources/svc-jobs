mod support;

use serde_json::json;
use support::db::{self, Durable};
use support::fixture::{JobsFixture, runner_access};
use support::gesture::{self, Mail, Work};
use support::lineage::{create, live_runner, started_with_agent};
use support::producer::{JobDeclaration, Producer};
use support::restart::{FINISHED_OF_RUN, RESTARTS_OF_RUN, save_state, upload_result};
use support::runner;
use support::{LONG, QUIET, gql, wire};

const IN_TREE: &str = "SELECT count(*) FROM directory_agents WHERE initial_job_id = $1";
const ITEMS_OF_RUN: &str = "SELECT count(*) FROM inbox_items WHERE run_id = $1";
const ITEMS_AT: &str = "SELECT count(*) FROM inbox_items WHERE address = $1";

#[tokio::test]
async fn a_failed_run_takes_its_agents_their_state_and_their_unread_items_and_a_mail_to_them_is_refused()
 {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let gql = fixture.gql();
    let (mut x, mut y) = (
        live_runner(&fixture, "removal-x").await,
        live_runner(&fixture, "removal-y").await,
    );
    let a = JobDeclaration::new(&x.runner_type).with_max_attempts(2);
    Producer::new(fixture.fabric(), "projects")
        .declare_v2(&a)
        .await;
    let (first, a1) = started_with_agent(&fixture, &mut x).await;
    let run1 = Work::of(&x, &first);
    create(
        &fixture,
        &run1,
        &y.runner_type,
        a.job_id,
        json!({ "mode": "SYNC" }),
    )
    .await;
    let (b_trigger, b1) = started_with_agent(&fixture, &mut y).await;
    let run2 = Work::of(&y, &b_trigger);
    upload_result(&fixture, &run2, "an answer").await;
    save_state(&fixture, &run2, json!({ "turn": 1 })).await;
    y.complete_run(&b_trigger).await;
    durable
        .await_count(FINISHED_OF_RUN, run2.run_id, 1, "B1 finished, restartable")
        .await;
    durable
        .await_count(
            ITEMS_OF_RUN,
            run1.run_id,
            1,
            "a notice waits unread for Run 1",
        )
        .await;
    let a2_agent = support::surface::Agent::with_inbox();
    let a2 = a2_agent.agent_id;
    support::surface::register(fixture.nats(), &x, run1.run_id, &[a2_agent]).await;
    let copy = Mail::new(a1, &[a2]).cc(&[b1]);
    gesture::accepted(
        &gesture::send_mail(&gql, &runner_access(), &run1, &copy).await,
        "jobsSendMail",
    );
    durable
        .assert_count(ITEMS_AT, b1, 1, "a cc notice waits unread at B1")
        .await;

    x.fail_run(&first, "TRANSIENT", "provider_unavailable", Some(0))
        .await;
    durable
        .await_count(
            IN_TREE,
            a.job_id,
            0,
            "A1, B1 and their Runs leave the directory",
        )
        .await;
    durable
        .assert_count(
            db::SAVED_STATES_OF_RUN,
            run2.run_id,
            0,
            "with their saved state",
        )
        .await;
    durable
        .assert_count(
            ITEMS_OF_RUN,
            run1.run_id,
            0,
            "the unread notices of the removed Run go",
        )
        .await;
    durable
        .assert_count(ITEMS_AT, b1, 0, "and those of its removed addresses")
        .await;

    let (retry, retried_agent) = started_with_agent(&fixture, &mut x).await;
    let refused = gesture::send_mail(
        &gql,
        &runner_access(),
        &Work::of(&x, &retry),
        &Mail::new(retried_agent, &[b1]),
    )
    .await;
    assert_eq!(gesture::refusal(&refused, "jobsSendMail"), "bad_addresses");
    assert_eq!(
        gesture::field(&refused, "jobsSendMail")["badAddresses"][0]["reason"],
        "recipient_removed"
    );
    y.expect_no_trigger(QUIET).await;
    durable
        .assert_count(RESTARTS_OF_RUN, run2.run_id, 0, "and starts no restart")
        .await;
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn the_cancel_of_a_terminal_job_removes_its_runs_and_agents_and_cancels_what_still_lives_below()
 {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let gql = fixture.gql();
    let producer = Producer::new(fixture.fabric(), "projects");
    let (mut x, mut y) = (
        live_runner(&fixture, "terminal-x").await,
        live_runner(&fixture, "terminal-y").await,
    );
    let r = JobDeclaration::new(&x.runner_type).with_max_attempts(1);
    producer.declare_v2(&r).await;
    let (r_trigger, _) = started_with_agent(&fixture, &mut x).await;
    let r_work = Work::of(&x, &r_trigger);
    let d = create(
        &fixture,
        &r_work,
        &y.runner_type,
        r.job_id,
        json!({ "mode": "SYNC" }),
    )
    .await;
    let (d_trigger, _) = started_with_agent(&fixture, &mut y).await;
    x.complete_run(&r_trigger).await;
    durable
        .await_count(FINISHED_OF_RUN, r_work.run_id, 1, "R's agent finished")
        .await;
    producer
        .send_v3(wire::VERB_FINISH, json!({ "job_id": r.job_id.to_string() }))
        .await;
    gql::wait_for_status(&gql, fixture.admin(), r.job_id, "COMPLETED", LONG).await;

    producer
        .send_v3(wire::VERB_CANCEL, json!({ "job_id": r.job_id.to_string() }))
        .await;
    gql::wait_for_status(&gql, fixture.admin(), d, "CANCELLED", LONG).await;
    durable
        .await_count(
            IN_TREE,
            r.job_id,
            0,
            "R's Runs and agents and D's leave the directory",
        )
        .await;
    y.await_cancel_entry(runner::run_id(&d_trigger), LONG).await;
    assert_eq!(
        gql::status_of(&gql, fixture.admin(), r.job_id).await,
        "COMPLETED"
    );
    durable.close().await;
    fixture.shutdown().await;
}
