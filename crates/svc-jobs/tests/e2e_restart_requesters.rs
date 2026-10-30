mod support;

use serde_json::json;
use support::call_notices::{background_call, caller};
use support::db::Durable;
use support::events::EventLog;
use support::fixture::{JobsFixture, runner_access};
use support::gesture::{self, Work};
use support::producer::{JobDeclaration, Producer};
use support::restart::{FINISHED_OF_RUN, read_result, register_same, result_for, retry_manually};
use support::restart::{save_state, upload_result};
use support::runner;
use support::surface::{self, Agent};
use support::tree::{call_child, pair};
use support::{LONG, wire};
use uuid::Uuid;

#[tokio::test]
async fn a_background_call_keeps_no_result() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let mut calling = caller(&fixture, "background-answer").await;
    let trigger = background_call(&fixture, &mut calling).await;
    calling.pair.child.start_applied(&fixture, &trigger).await;
    let work = Work::of(&calling.pair.child, &trigger);

    let refused = fixture
        .gql()
        .query(
            &runner_access(),
            gesture::UPLOAD_RESULT,
            json!({ "input": { "work": work.wire(), "answer": gesture::content("late", &[]) } }),
        )
        .await;

    assert_eq!(
        gesture::refusal(&refused, "jobsUploadResult"),
        "not_sync_call"
    );
    let answerer = support::surface::Agent::with_inbox();
    let answerer_id = answerer.agent_id;
    support::surface::register(
        fixture.nats(),
        &calling.pair.child,
        work.run_id,
        &[answerer],
    )
    .await;
    let answer =
        gesture::Mail::new(answerer_id, &[calling.address]).with_body("the answer by mail");
    gesture::accepted(
        &gesture::send_mail(&fixture.gql(), &runner_access(), &work, &answer).await,
        "jobsSendMail",
    );
    let mails: Vec<serde_json::Value> = gesture::items(
        &gesture::unread(
            &fixture.gql(),
            &runner_access(),
            &calling.work,
            Some(calling.address),
        )
        .await,
    )
    .into_iter()
    .filter(|item| item["itemKind"] == "MAIL")
    .collect();
    assert_eq!(
        mails.len(),
        1,
        "the answer waits at the address the call named"
    );
    assert_eq!(mails[0]["body"], "the answer by mail");
    assert_eq!(mails[0]["from"], answerer_id.to_string());
    let read = fixture
        .gql()
        .query(
            &runner_access(),
            gesture::READ_RESULT,
            json!({ "input": { "work": calling.work.wire(), "resultId": Uuid::now_v7().to_string(), "keep": false } }),
        )
        .await;
    assert_eq!(
        gesture::refusal(&read, "jobsReadResult"),
        "result_unknown",
        "there is no Result to read"
    );
    durable
        .assert_count(
            "SELECT count(*) FROM job_results WHERE job_id = $1",
            work.job_id,
            0,
            "a background call keeps no Result",
        )
        .await;
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_manual_retry_of_the_caller_does_not_read_the_result_addressed_to_the_run_it_retries() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let mut pair = pair(&fixture, "retry-result").await;
    let producer = Producer::new(fixture.fabric(), "projects");
    let root = JobDeclaration::new(&pair.parent.runner_type).with_max_attempts(1);
    producer.declare_v2(&root).await;
    let caller_trigger = pair.parent.next_trigger(LONG).await;
    pair.parent.start_applied(&fixture, &caller_trigger).await;
    let call = call_child(
        &fixture,
        &mut pair,
        root.job_id,
        runner::run_id(&caller_trigger),
    )
    .await;
    pair.child.start_applied(&fixture, &call).await;
    let call_work = Work::of(&pair.child, &call);
    upload_result(&fixture, &call_work, "answer").await;
    let result = result_for(&durable, call_work.job_id).await;
    pair.parent
        .fail_run(&caller_trigger, "PERMANENT", "boom_after_the_answer", None)
        .await;
    durable
        .assert_count(
            "SELECT count(*) FROM job_results WHERE result_id = $1",
            result,
            1,
            "the answer outlives the failure of the Run it is addressed to",
        )
        .await;

    let successor = retry_manually(&fixture, root.job_id).await;
    let retry = pair.parent.next_trigger(LONG).await;
    assert_eq!(runner::job_id(&retry), successor);
    pair.parent.start_applied(&fixture, &retry).await;
    let read = read_result(&fixture, &Work::of(&pair.parent, &retry), result).await;

    assert_eq!(
        gesture::refusal(&read, "jobsReadResult"),
        "not_addressed_to_you",
        "a retry of the Caller does not read what was addressed to the Run it retries"
    );
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn the_manual_retry_of_a_restart_job_is_still_a_restart_that_acts_for_the_same_runs() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let mut pair = pair(&fixture, "retry-restart").await;
    let producer = Producer::new(fixture.fabric(), "projects");
    let root = JobDeclaration::new(&pair.parent.runner_type).with_max_attempts(1);
    producer.declare_v2(&root).await;
    let first = pair.parent.next_trigger(LONG).await;
    pair.parent.start_applied(&fixture, &first).await;
    let first_run = runner::run_id(&first);
    let speaker = Uuid::now_v7();
    register_same(&fixture, &pair, first_run, speaker).await;
    save_state(
        &fixture,
        &Work::of(&pair.parent, &first),
        json!({ "turn": 1 }),
    )
    .await;
    let one = call_child(&fixture, &mut pair, root.job_id, first_run).await;
    pair.child.start_applied(&fixture, &one).await;
    let one_work = Work::of(&pair.child, &one);
    let caller_agent = Uuid::now_v7();
    surface::register(
        fixture.nats(),
        &pair.child,
        runner::run_id(&one),
        &[Agent::with_inbox().with_id(caller_agent)],
    )
    .await;
    upload_result(&fixture, &one_work, "first answer").await;
    let answer = result_for(&durable, one_work.job_id).await;
    pair.parent.complete_run(&first).await;
    durable
        .await_count(FINISHED_OF_RUN, first_run, 1, "the speaker finishes")
        .await;
    gesture::accepted(
        &gesture::send_mail(
            &fixture.gql(),
            &runner_access(),
            &one_work,
            &gesture::Mail::new(caller_agent, &[speaker]),
        )
        .await,
        "jobsSendMail",
    );
    let second = pair.parent.next_trigger(LONG).await;
    pair.parent.start_applied(&fixture, &second).await;
    pair.parent
        .fail_run(&second, "PERMANENT", "boom", None)
        .await;

    let successor = retry_manually(&fixture, runner::job_id(&second)).await;
    let retry = pair.parent.next_trigger(LONG).await;
    assert_eq!(runner::job_id(&retry), successor);
    assert_eq!(
        retry["restart"]["restart_of_run_id"],
        json!(first_run.to_string()),
        "the successor of a restart Job restarts the same Run"
    );
    pair.parent.start_applied(&fixture, &retry).await;
    let retry_work = Work::of(&pair.parent, &retry);

    let read = read_result(&fixture, &retry_work, answer).await;
    assert_eq!(
        gesture::field(&read, "jobsReadResult")["content"]["message"],
        "first answer",
        "the successor acts for the Run the failed restart acted for"
    );
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_service_requester_learns_of_the_restart_job_from_its_events() {
    let fixture = JobsFixture::start().await;
    let events = EventLog::open_v2(fixture.fabric()).await;
    let durable = Durable::open(&fixture.app_url()).await;
    let mut pair = pair(&fixture, "restart-service").await;
    let producer = Producer::new(fixture.fabric(), "projects");
    let root = JobDeclaration::new(&pair.parent.runner_type).with_max_attempts(1);
    producer.declare_v2(&root).await;
    let first = pair.parent.next_trigger(LONG).await;
    pair.parent.start_applied(&fixture, &first).await;
    let first_run = runner::run_id(&first);
    let speaker = Uuid::now_v7();
    register_same(&fixture, &pair, first_run, speaker).await;
    save_state(
        &fixture,
        &Work::of(&pair.parent, &first),
        json!({ "turn": 1 }),
    )
    .await;
    let one = call_child(&fixture, &mut pair, root.job_id, first_run).await;
    pair.child.start_applied(&fixture, &one).await;
    let caller_agent = Uuid::now_v7();
    surface::register(
        fixture.nats(),
        &pair.child,
        runner::run_id(&one),
        &[Agent::with_inbox().with_id(caller_agent)],
    )
    .await;
    pair.parent.complete_run(&first).await;
    durable
        .await_count(FINISHED_OF_RUN, first_run, 1, "the speaker finishes")
        .await;

    gesture::accepted(
        &gesture::send_mail(
            &fixture.gql(),
            &runner_access(),
            &Work::of(&pair.child, &one),
            &gesture::Mail::new(caller_agent, &[speaker]),
        )
        .await,
        "jobsSendMail",
    );
    let second = pair.parent.next_trigger(LONG).await;
    let restart_job = runner::job_id(&second);

    let has_service_requester: bool =
        sqlx::query_scalar("SELECT jsonb_exists(requester, 'Service') FROM jobs WHERE id = $1")
            .bind(restart_job)
            .fetch_one(durable.pool())
            .await
            .expect("the restart Job reads");
    assert!(
        has_service_requester,
        "the restart answers to the service that asked for the Job it restarts"
    );
    let queued = events
        .expect_one(wire::FACT_QUEUED, restart_job, LONG)
        .await;
    assert_eq!(
        queued.payload()["restart_of_job_id"],
        json!(root.job_id.to_string()),
        "the event names the Job the restart stems from, so the service can link it"
    );
    assert_eq!(queued.payload()["requester"]["kind"], "SERVICE");
    pair.parent.start_applied(&fixture, &second).await;
    events
        .expect_one(wire::FACT_STARTED, restart_job, LONG)
        .await;
    events.stop().await;
    durable.close().await;
    fixture.shutdown().await;
}
