mod support;

use serde_json::json;
use support::LONG;
use support::call_notices::{background_call, caller, types, wait_for_notice};
use support::db::Durable;
use support::fixture::{JobsFixture, runner_access};
use support::gesture::{self, Work};
use support::restart::{FINISHED_OF_RUN, RESTARTS_OF_RUN};
use support::runner;
use support::surface::{self, Agent};

#[tokio::test]
async fn a_failure_notice_wakes_a_finished_background_caller() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let mut caller = caller(&fixture, "notices-wake").await;
    let parent_run = runner::run_id(&caller.trigger);
    let saved = fixture
        .gql()
        .query(
            &runner_access(),
            gesture::SAVE_STATE,
            json!({ "input": { "work": caller.work.wire(), "state": { "turn": 1 } } }),
        )
        .await;
    gesture::accepted(&saved, "jobsSaveState");

    let call = background_call(&fixture, &mut caller).await;
    caller.pair.child.start_applied(&fixture, &call).await;
    caller.pair.parent.complete_run(&caller.trigger).await;
    durable
        .await_count(FINISHED_OF_RUN, parent_run, 1, "the caller finishes")
        .await;
    caller.pair.parent.expect_no_trigger(support::QUIET).await;

    caller
        .pair
        .child
        .fail_run(&call, "PERMANENT", "boom", None)
        .await;
    let restart = caller.pair.parent.next_trigger(LONG).await;
    assert_eq!(
        restart["restart"]["restart_of_run_id"],
        json!(parent_run.to_string())
    );
    durable
        .assert_count(RESTARTS_OF_RUN, parent_run, 1, "one restart")
        .await;
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_notice_for_an_ended_caller_run_reaches_the_live_restart_run_that_acts_for_it() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let mut caller = caller(&fixture, "notices-acts-for").await;
    let parent_run = runner::run_id(&caller.trigger);
    let saved = fixture
        .gql()
        .query(
            &runner_access(),
            gesture::SAVE_STATE,
            json!({ "input": { "work": caller.work.wire(), "state": { "turn": 1 } } }),
        )
        .await;
    gesture::accepted(&saved, "jobsSaveState");

    let call = background_call(&fixture, &mut caller).await;
    caller.pair.child.start_applied(&fixture, &call).await;
    let call_work = Work::of(&caller.pair.child, &call);
    let messenger = Agent::with_inbox();
    let messenger_id = messenger.agent_id;
    surface::register(
        fixture.nats(),
        &caller.pair.child,
        runner::run_id(&call),
        &[messenger],
    )
    .await;
    caller.pair.parent.complete_run(&caller.trigger).await;
    durable
        .await_count(FINISHED_OF_RUN, parent_run, 1, "the caller finishes")
        .await;

    let wake = gesture::Mail::new(messenger_id, &[caller.address]);
    gesture::accepted(
        &gesture::send_mail(&fixture.gql(), &runner_access(), &call_work, &wake).await,
        "jobsSendMail",
    );
    let restart = caller.pair.parent.next_trigger(LONG).await;
    caller.pair.parent.start_applied(&fixture, &restart).await;
    let restart_work = Work::of(&caller.pair.parent, &restart);

    caller.pair.child.complete_run(&call).await;
    let seen = wait_for_notice(
        &fixture,
        &restart_work,
        Some(caller.address),
        "RUN_COMPLETED",
    )
    .await;
    assert!(
        types(&seen).contains(&"ACCEPTED".to_owned()),
        "the restart Run reads the notices that waited for the ended Run: {seen:?}"
    );
    let rings: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM integration_outbox WHERE payload::text LIKE '%item_arrived%' \
         AND payload::text LIKE $1 AND payload::text LIKE '%NOTICE%'",
    )
    .bind(format!("%{}%", runner::run_id(&restart)))
    .fetch_one(durable.pool())
    .await
    .expect("the doorbell staging reads");
    assert!(
        rings >= 1,
        "the live restart Run is rung for a notice of the Run it acts for"
    );
    durable.close().await;
    fixture.shutdown().await;
}
