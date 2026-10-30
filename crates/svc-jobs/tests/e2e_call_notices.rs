mod support;

use serde_json::{Value, json};
use support::db::Durable;
use support::fixture::{JobsFixture, runner_access};
use support::gesture::{self, Work};
use support::producer::{JobDeclaration, Producer};
use support::restart::{FINISHED_OF_RUN, RESTARTS_OF_RUN};
use support::runner;
use support::surface::{self, Agent};
use support::tree::{Pair, call_child, pair};
use support::{LONG, wire};
use uuid::Uuid;

struct Caller<'a> {
    pair: Pair<'a>,
    root: Uuid,
    trigger: Value,
    work: Work,
    address: Uuid,
}

async fn caller<'a>(fixture: &'a JobsFixture, label: &str) -> Caller<'a> {
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut pair = pair(fixture, label).await;
    let root = JobDeclaration::new(&pair.parent.runner_type).with_max_attempts(1);
    producer.declare_v2(&root).await;
    let trigger = pair.parent.next_trigger(LONG).await;
    pair.parent.start_applied(fixture, &trigger).await;
    let agent = Agent::with_inbox();
    let address = agent.agent_id;
    surface::register(
        fixture.nats(),
        &pair.parent,
        runner::run_id(&trigger),
        &[agent],
    )
    .await;
    Caller {
        work: Work::of(&pair.parent, &trigger),
        pair,
        root: root.job_id,
        trigger,
        address,
    }
}

async fn background_call(fixture: &JobsFixture, caller: &mut Caller<'_>) -> Value {
    let producer = Producer::new(fixture.fabric(), "projects");
    producer
        .send_v3(
            wire::VERB_CREATE,
            json!({
                "job_id": Uuid::now_v7().to_string(),
                "runner_type": caller.pair.child.runner_type,
                "producer": "projects",
                "parent_job_id": caller.root.to_string(),
                "mode": "BACKGROUND",
                "caller_address": caller.address.to_string(),
                "acting_run_id": runner::run_id(&caller.trigger).to_string(),
                "acting_instance_key": caller.pair.parent.instance_key,
                "max_attempts": 1,
            }),
        )
        .await;
    caller.pair.child.next_trigger(LONG).await
}

async fn notices(fixture: &JobsFixture, work: &Work, address: Option<Uuid>) -> Vec<Value> {
    gesture::items(&gesture::unread(&fixture.gql(), &runner_access(), work, address).await)
        .into_iter()
        .filter(|item| item["itemKind"] == "NOTICE")
        .collect()
}

fn types(items: &[Value]) -> Vec<String> {
    items
        .iter()
        .map(|item| item["noticeType"].as_str().unwrap_or_default().to_owned())
        .collect()
}

async fn wait_for_notice(
    fixture: &JobsFixture,
    work: &Work,
    address: Option<Uuid>,
    notice_type: &str,
) -> Vec<Value> {
    wait_for_job_notice(fixture, work, address, notice_type, None).await
}

async fn wait_for_job_notice(
    fixture: &JobsFixture,
    work: &Work,
    address: Option<Uuid>,
    notice_type: &str,
    job_id: Option<Uuid>,
) -> Vec<Value> {
    let waited = std::time::Instant::now();
    loop {
        let seen = notices(fixture, work, address).await;
        if seen.iter().any(|item| {
            item["noticeType"] == notice_type
                && job_id.is_none_or(|job| item["jobId"] == json!(job.to_string()))
        }) {
            return seen;
        }
        assert!(
            waited.elapsed() < LONG,
            "no {notice_type} notice arrived; saw {seen:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn a_background_call_tells_its_caller_every_step_at_the_address_it_named() {
    let fixture = JobsFixture::start().await;
    let mut caller = caller(&fixture, "notices-background").await;

    let first = background_call(&fixture, &mut caller).await;
    let seen = wait_for_notice(
        &fixture,
        &caller.work,
        Some(caller.address),
        "QUEUE_POSITION",
    )
    .await;
    assert_eq!(
        types(&seen),
        vec!["ACCEPTED", "QUEUE_POSITION"],
        "the call is confirmed at once, then its place in the wait is told"
    );
    assert_eq!(seen[1]["position"], json!(1));
    assert_eq!(seen[0]["jobId"], json!(runner::job_id(&first).to_string()));

    caller.pair.child.start_applied(&fixture, &first).await;
    wait_for_notice(&fixture, &caller.work, Some(caller.address), "STARTED").await;
    caller.pair.child.complete_run(&first).await;
    let done = wait_for_notice(
        &fixture,
        &caller.work,
        Some(caller.address),
        "RUN_COMPLETED",
    )
    .await;
    assert_eq!(
        types(&done),
        vec!["ACCEPTED", "QUEUE_POSITION", "STARTED", "RUN_COMPLETED"]
    );
    assert!(
        done[3]["answer"].is_null() && done[3]["resultId"].is_null(),
        "a background call keeps no Result: the answer comes by mail"
    );

    let second = background_call(&fixture, &mut caller).await;
    caller.pair.child.start_applied(&fixture, &second).await;
    caller
        .pair
        .child
        .fail_run(&second, "PERMANENT", "provider_refused", None)
        .await;
    let failed = wait_for_notice(&fixture, &caller.work, Some(caller.address), "JOB_FAILED").await;
    let report = failed
        .iter()
        .find(|item| item["noticeType"] == "JOB_FAILED")
        .expect("the failure notice");
    assert_eq!(report["failureReport"]["reason_code"], "provider_refused");
    assert!(
        report["message"]
            .as_str()
            .is_some_and(|message| message.contains("provider_refused")),
        "the failure comes with a message in plain words: {report}"
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_sync_call_tells_the_callers_run_the_end_with_the_answer_or_a_reference_and_the_failure()
{
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let mut caller = caller(&fixture, "notices-sync").await;
    let parent_run = runner::run_id(&caller.trigger);

    let small = call_child(&fixture, &mut caller.pair, caller.root, parent_run).await;
    caller.pair.child.start_applied(&fixture, &small).await;
    let small_work = Work::of(&caller.pair.child, &small);
    let uploaded = fixture
        .gql()
        .query(
            &runner_access(),
            gesture::UPLOAD_RESULT,
            json!({ "input": { "work": small_work.wire(), "answer": gesture::content("42", &[]) } }),
        )
        .await;
    gesture::accepted(&uploaded, "jobsUploadResult");
    caller.pair.child.complete_run(&small).await;
    let seen = wait_for_notice(&fixture, &caller.work, None, "RUN_COMPLETED").await;
    assert_eq!(
        types(&seen),
        vec!["RUN_COMPLETED"],
        "a sync call tells only the end"
    );
    assert_eq!(seen[0]["answer"]["message"], "42");
    assert!(
        seen[0]["resultId"].is_string(),
        "the Result stays readable by reference"
    );

    let large = call_child(&fixture, &mut caller.pair, caller.root, parent_run).await;
    caller.pair.child.start_applied(&fixture, &large).await;
    let large_work = Work::of(&caller.pair.child, &large);
    let uploaded = fixture
        .gql()
        .query(
            &runner_access(),
            gesture::UPLOAD_RESULT,
            json!({ "input": { "work": large_work.wire(), "answer": gesture::content(&"x".repeat(600_000), &[]) } }),
        )
        .await;
    gesture::accepted(&uploaded, "jobsUploadResult");
    caller.pair.child.complete_run(&large).await;
    let both = wait_for_job_notice(
        &fixture,
        &caller.work,
        None,
        "RUN_COMPLETED",
        Some(large_work.job_id),
    )
    .await;
    let reference_only = both
        .iter()
        .find(|item| item["jobId"] == json!(large_work.job_id.to_string()))
        .expect("the notice of the large call");
    assert!(
        reference_only["answer"].is_null(),
        "above the bound, only a reference"
    );
    assert!(reference_only["resultId"].is_string());

    let doomed = call_child(&fixture, &mut caller.pair, caller.root, parent_run).await;
    caller.pair.child.start_applied(&fixture, &doomed).await;
    caller
        .pair
        .child
        .fail_run(&doomed, "PERMANENT", "boom", None)
        .await;
    let failed = wait_for_notice(&fixture, &caller.work, None, "JOB_FAILED").await;
    assert!(
        failed.iter().any(|item| item["noticeType"] == "JOB_FAILED"
            && item["failureReport"]["reason_code"] == "boom")
    );
    durable.close().await;
    fixture.shutdown().await;
}

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
