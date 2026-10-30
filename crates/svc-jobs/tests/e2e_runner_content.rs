mod support;

use br_test_harness::PassportBuilder;
use serde_json::json;
use support::db::Durable;
use support::fixture::{JobsFixture, runner_access};
use support::gesture::{self, Work};
use support::producer::{JobDeclaration, Producer};
use support::runner::{self, FakeRunner};
use support::surface::{self, Agent};
use support::tree::{call_child, correspondents, pair};
use support::{LONG, dispatch, wire};
use uuid::Uuid;

#[tokio::test]
async fn a_payload_uploaded_before_its_job_exists_is_fetched_only_by_the_runner_that_works_the_job()
{
    let fixture = JobsFixture::start().await;
    let gql = fixture.gql();
    let producer = Producer::new(fixture.fabric(), "projects");
    let uploader = PassportBuilder::new()
        .user_id(producer.account_id)
        .claim("scopes", vec!["jobs:runner"])
        .build_service();
    let runner_type = wire::unique_runner_type("kept-payload");
    let mut instance = FakeRunner::new(fixture.nats(), &runner_type, "instance-a");
    instance.connect().await;
    dispatch::await_live_instances(&gql, fixture.admin(), &runner_type, 1, LONG).await;
    let declaration = JobDeclaration::new(&runner_type);

    let stored =
        gesture::upload_payload(&gql, &uploader, declaration.job_id, "the whole brief", None).await;
    gesture::accepted(&stored, "jobsUploadPayload");
    let repeated =
        gesture::upload_payload(&gql, &uploader, declaration.job_id, "the whole brief", None).await;
    gesture::accepted(&repeated, "jobsUploadPayload");
    let changed =
        gesture::upload_payload(&gql, &uploader, declaration.job_id, "another brief", None).await;
    assert_eq!(
        gesture::refusal(&changed, "jobsUploadPayload"),
        "payload_already_uploaded"
    );

    let mut body = declaration.payload("projects");
    body["payload"] = json!({ "form": "KEPT" });
    producer.declare_payload(body).await;
    let trigger = instance.next_trigger(LONG).await;
    instance.start_applied(&fixture, &trigger).await;
    let work = Work::of(&instance, &trigger);

    let fetched = gesture::payload(&gql, &runner_access(), &work).await;
    let content = &gesture::field(&fetched, "jobsPayload")["content"];
    assert_eq!(content["message"], "the whole brief");
    assert_eq!(content["attachments"][0]["name"], "note.txt");

    let foreign = Work {
        job_id: Uuid::now_v7(),
        ..work.clone()
    };
    let refused = gesture::payload(&gql, &runner_access(), &foreign).await;
    assert_eq!(gesture::refusal(&refused, "jobsPayload"), "job_not_of_run");

    let administrator = gesture::payload(&gql, fixture.admin(), &work).await;
    assert!(
        administrator["errors"].is_array(),
        "administrators do not read kept payloads: {administrator}"
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_sync_answer_is_a_result_read_once_by_the_calling_run_unless_it_asks_to_keep_it() {
    let fixture = JobsFixture::start().await;
    let gql = fixture.gql();
    let runner = runner_access();
    let durable = Durable::open(&fixture.app_url()).await;
    let tree = correspondents(&fixture, "result").await;
    let answered = tree.child_work.job_id;

    let uploaded = gql
        .query(
            &runner,
            gesture::UPLOAD_RESULT,
            json!({ "input": {
                "work": tree.child_work.wire(),
                "answer": gesture::content("the answer", &[("out.txt", "text/plain", "42")]),
            } }),
        )
        .await;
    gesture::accepted(&uploaded, "jobsUploadResult");
    let result_id: Uuid =
        sqlx::query_scalar("SELECT result_id::uuid FROM job_results WHERE job_id = $1")
            .bind(answered)
            .fetch_one(durable.pool())
            .await
            .expect("the answer is kept as a Result");

    let read = |work: &Work, keep: bool| {
        gql.query(
            &runner,
            gesture::READ_RESULT,
            json!({ "input": { "work": work.wire(), "resultId": result_id.to_string(), "keep": keep } }),
        )
    };
    let stranger = read(&tree.child_work, false).await;
    assert_eq!(
        gesture::refusal(&stranger, "jobsReadResult"),
        "not_addressed_to_you"
    );
    let kept = read(&tree.parent_work, true).await;
    assert_eq!(
        gesture::field(&kept, "jobsReadResult")["content"]["message"],
        "the answer"
    );
    let again = read(&tree.parent_work, false).await;
    assert_eq!(
        gesture::field(&again, "jobsReadResult")["content"]["attachments"][0]["content"],
        "42"
    );
    let erased = read(&tree.parent_work, false).await;
    assert_eq!(
        gesture::refusal(&erased, "jobsReadResult"),
        "result_unknown"
    );

    let administrator = gql
        .query(
            fixture.admin(),
            gesture::READ_RESULT,
            json!({ "input": { "work": tree.parent_work.wire(), "resultId": result_id.to_string() } }),
        )
        .await;
    assert!(administrator["errors"].is_array(), "{administrator}");
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_mail_body_above_one_megabyte_is_delivered_whole() {
    let fixture = JobsFixture::start().await;
    let gql = fixture.gql();
    let runner = runner_access();
    let tree = correspondents(&fixture, "mail-large").await;
    let body = "x".repeat(1_500_000);

    let mail = gesture::Mail::new(tree.sender, &[tree.receiver]).with_body(&body);
    gesture::accepted(
        &gesture::send_mail(&gql, &runner, &tree.parent_work, &mail).await,
        "jobsSendMail",
    );
    let unread = gesture::items(
        &gesture::unread(&gql, &runner, &tree.child_work, Some(tree.receiver)).await,
    );
    assert_eq!(unread[0]["body"].as_str().map(str::len), Some(body.len()));
    fixture.shutdown().await;
}

#[tokio::test]
async fn an_agent_that_finished_after_a_save_is_listed_as_restartable() {
    let fixture = JobsFixture::start().await;
    let producer = Producer::new(fixture.fabric(), "projects");
    let durable = Durable::open(&fixture.app_url()).await;
    let mut tree = pair(&fixture, "restartable").await;
    let nats = fixture.nats();
    let root = JobDeclaration::new(&tree.parent.runner_type);
    producer.declare_v2(&root).await;
    let parent_trigger = tree.parent.next_trigger(LONG).await;
    tree.parent.start_applied(&fixture, &parent_trigger).await;
    let parent_run = runner::run_id(&parent_trigger);
    let agent = Agent::with_inbox();
    let agent_id = agent.agent_id;
    surface::register(nats, &tree.parent, parent_run, &[agent]).await;
    let called = call_child(&fixture, &mut tree, root.job_id, parent_run).await;
    tree.child.start_applied(&fixture, &called).await;
    let child_run = runner::run_id(&called);

    let saved = fixture
        .gql()
        .query(
            &runner_access(),
            gesture::SAVE_STATE,
            json!({ "input": {
                "work": Work::of(&tree.parent, &parent_trigger).wire(),
                "state": { "turn": 3 },
            } }),
        )
        .await;
    gesture::accepted(&saved, "jobsSaveState");
    durable
        .assert_count(
            "SELECT count(*) FROM saved_state_agents WHERE run_id = $1",
            parent_run,
            1,
            "a save keeps the agents the Run holds in the directory",
        )
        .await;
    let before = surface::list(nats, &tree.child, child_run).await;
    let live = before["agents"]
        .as_array()
        .and_then(|agents| {
            agents
                .iter()
                .find(|a| a["agent_id"] == agent_id.to_string())
        })
        .cloned()
        .unwrap_or_else(|| panic!("the running agent is listed: {before}"));
    assert_eq!(live["state"], "RUNNING");
    assert!(live["restartable_now"].is_null());

    tree.parent.complete_run(&parent_trigger).await;
    durable
        .await_count(
            "SELECT count(*) FROM directory_agents WHERE run_id = $1 AND state = 'FINISHED'",
            parent_run,
            1,
            "the agent of the ended run finishes",
        )
        .await;
    let after = surface::list(nats, &tree.child, child_run).await;
    let listed = after["agents"]
        .as_array()
        .and_then(|agents| {
            agents
                .iter()
                .find(|a| a["agent_id"] == agent_id.to_string())
        })
        .cloned()
        .unwrap_or_else(|| panic!("the finished agent with a saved state is listed: {after}"));
    assert_eq!(listed["state"], "FINISHED");
    assert_eq!(listed["restartable_now"], true);
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn any_run_saves_a_state_within_the_limit_and_only_the_run_of_a_restart_job_reads_one() {
    let fixture = JobsFixture::start().await;
    let gql = fixture.gql();
    let runner = runner_access();
    let tree = correspondents(&fixture, "state-plain").await;
    let save = |state: serde_json::Value| {
        gql.query(
            &runner,
            gesture::SAVE_STATE,
            json!({ "input": { "work": tree.parent_work.wire(), "state": state } }),
        )
    };

    gesture::accepted(&save(json!({ "turn": 3 })).await, "jobsSaveState");
    let oversized = save(json!({ "log": "x".repeat(1_200_000) })).await;
    assert_eq!(
        gesture::refusal(&oversized, "jobsSaveState"),
        "state_too_large"
    );

    let read = gql
        .query(
            &runner,
            gesture::SAVED_STATE,
            json!({ "work": tree.parent_work.wire() }),
        )
        .await;
    assert_eq!(
        gesture::refusal(&read, "jobsSavedState"),
        "not_a_restart_run"
    );
    let administrator = gql
        .query(
            fixture.admin(),
            gesture::SAVED_STATE,
            json!({ "work": tree.parent_work.wire() }),
        )
        .await;
    assert!(administrator["errors"].is_array(), "{administrator}");
    fixture.shutdown().await;
}
