mod support;

use serde_json::{Value, json};
use support::call_notices::{Caller, background_call, caller, notices, wait_for_job_notice};
use support::db::Durable;
use support::fixture::JobsFixture;
use support::gesture::Work;
use support::producer::Producer;
use support::restart::{FINISHED_OF_RUN, register_same, save_state};
use support::{LONG, QUIET, runner, wire};
use uuid::Uuid;

const REFUSALS_OF_RUN: &str = "SELECT count(*) FROM inbox_items \
     WHERE kind = 'NOTICE' AND run_id = $1 AND notice ->> 'type' = 'REFUSED'";

fn refused_call(caller: &Caller<'_>, run: &Value, job: Uuid, address: Uuid) -> Value {
    json!({
        "job_id": job.to_string(),
        "runner_type": caller.pair.child.runner_type,
        "producer": "projects",
        "mode": "BACKGROUND",
        "caller_address": address.to_string(),
        "acting_run_id": runner::run_id(run).to_string(),
        "acting_instance_key": caller.pair.parent.instance_key,
        "max_attempts": 1,
        "priority": 99,
    })
}

#[tokio::test]
async fn a_refused_call_of_a_restart_run_leaves_its_refusal_notice_at_the_restart_run() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let mut caller = caller(&fixture, "refusal-restart").await;
    let first_run = runner::run_id(&caller.trigger);
    save_state(&fixture, &caller.work, json!({ "turn": 1 })).await;
    let call = background_call(&fixture, &mut caller).await;
    caller.pair.child.start_applied(&fixture, &call).await;
    caller.pair.parent.complete_run(&caller.trigger).await;
    durable
        .await_count(FINISHED_OF_RUN, first_run, 1, "the caller finishes")
        .await;
    caller
        .pair
        .child
        .fail_run(&call, "PERMANENT", "boom", None)
        .await;
    let restart = caller.pair.parent.next_trigger(LONG).await;
    caller.pair.parent.start_applied(&fixture, &restart).await;
    let restarted = Work::of(&caller.pair.parent, &restart);
    let restart_run = runner::run_id(&restart);
    register_same(&fixture, &caller.pair, restart_run, caller.address).await;

    let job = Uuid::now_v7();
    Producer::new(fixture.fabric(), "projects")
        .send_v3(
            wire::VERB_CREATE,
            refused_call(&caller, &restart, job, caller.address),
        )
        .await;
    wait_for_job_notice(
        &fixture,
        &restarted,
        Some(caller.address),
        "REFUSED",
        Some(job),
    )
    .await;
    durable
        .assert_count(
            REFUSALS_OF_RUN,
            restart_run,
            1,
            "the restart Run holds the refusal notice",
        )
        .await;
    durable
        .assert_count(
            REFUSALS_OF_RUN,
            first_run,
            0,
            "the ended Run it acts for holds none",
        )
        .await;
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_call_naming_an_address_held_by_another_run_leaves_no_refusal_notice_at_either_run() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let mine = caller(&fixture, "refusal-mine").await;
    let other = caller(&fixture, "refusal-other").await;
    let mine_run = runner::run_id(&mine.trigger);
    let other_run = runner::run_id(&other.trigger);

    let job = Uuid::now_v7();
    Producer::new(fixture.fabric(), "projects")
        .send_v3(
            wire::VERB_CREATE,
            refused_call(&mine, &mine.trigger, job, other.address),
        )
        .await;
    tokio::time::sleep(QUIET).await;
    for (run, what) in [
        (other_run, "the Run that holds the address"),
        (mine_run, "the Run that named it"),
    ] {
        durable.assert_count(REFUSALS_OF_RUN, run, 0, what).await;
    }
    assert!(
        notices(&fixture, &other.work, Some(other.address))
            .await
            .iter()
            .all(|item| item["noticeType"] != "REFUSED"),
        "the holder of the address reads no refusal of a call it never made"
    );
    durable.close().await;
    fixture.shutdown().await;
}
