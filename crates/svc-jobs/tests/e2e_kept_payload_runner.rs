mod support;

use serde_json::{Value, json};
use support::db::Durable;
use support::events::EventLog;
use support::fixture::{JobsFixture, runner_access};
use support::gesture::{self, Mail, Work};
use support::producer::{JobDeclaration, Producer};
use support::restart::{FINISHED_OF_RUN, save_state};
use support::runner::{self, FakeRunner};
use support::surface::{self, Agent};
use support::tree::pair;
use support::{LONG, dispatch, wire};
use uuid::Uuid;

const KEPT_OF_JOB: &str = "SELECT count(*) FROM kept_payloads WHERE job_id = $1";

struct Caller {
    work: Work,
    root: Uuid,
    address: Uuid,
}

async fn live_root(fixture: &JobsFixture, runner: &mut FakeRunner<'_>) -> Caller {
    let producer = Producer::new(fixture.fabric(), "projects");
    let root = JobDeclaration::new(&runner.runner_type).with_max_attempts(1);
    producer.declare_v2(&root).await;
    let trigger = runner.next_trigger(LONG).await;
    runner.start_applied(fixture, &trigger).await;
    let agent = Agent::with_inbox();
    let address = agent.agent_id;
    surface::register(fixture.nats(), runner, runner::run_id(&trigger), &[agent]).await;
    Caller {
        work: Work::of(runner, &trigger),
        root: root.job_id,
        address,
    }
}

async fn create_kept(fixture: &JobsFixture, caller: &Caller, runner_type: &str, job_id: Uuid) {
    Producer::new(fixture.fabric(), "projects")
        .send_v3(
            wire::VERB_CREATE,
            json!({
                "job_id": job_id.to_string(),
                "runner_type": runner_type,
                "producer": "projects",
                "parent_job_id": caller.root.to_string(),
                "mode": "SYNC",
                "max_attempts": 2,
                "payload": { "form": "KEPT" },
                "acting_run_id": caller.work.run_id.to_string(),
                "acting_instance_key": caller.work.instance_key,
            }),
        )
        .await;
}

async fn upload(fixture: &JobsFixture, caller: &Caller, job_id: Uuid, message: &str) -> Value {
    gesture::upload_payload(
        &fixture.gql(),
        &runner_access(),
        job_id,
        message,
        Some(&caller.work),
    )
    .await
}

async fn fetched_message(fixture: &JobsFixture, work: &Work) -> Value {
    let fetched = gesture::payload(&fixture.gql(), &runner_access(), work).await;
    gesture::field(&fetched, "jobsPayload")["content"]["message"].clone()
}

async fn other_runner<'a>(fixture: &'a JobsFixture, label: &str) -> FakeRunner<'a> {
    let mut other = FakeRunner::new(
        fixture.nats(),
        &wire::unique_runner_type(label),
        "instance-d",
    )
    .declaring_capacity(3);
    other.connect().await;
    dispatch::await_live_instances(&fixture.gql(), fixture.admin(), &other.runner_type, 1, LONG)
        .await;
    other
}

#[tokio::test]
async fn a_runner_uploads_a_kept_payload_before_its_job_and_every_run_of_the_job_fetches_it() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let answers = EventLog::open_v2(fixture.fabric()).await;
    let mut pair = pair(&fixture, "kept-by-runner").await;
    let caller = live_root(&fixture, &mut pair.parent).await;
    let mut d_runner = other_runner(&fixture, "kept-d").await;
    let d = live_root(&fixture, &mut d_runner).await;
    let job = Uuid::now_v7();

    gesture::accepted(
        &upload(&fixture, &caller, job, "the brief").await,
        "jobsUploadPayload",
    );
    create_kept(&fixture, &caller, &pair.child.runner_type, job).await;
    let first = pair.child.next_trigger(LONG).await;
    assert_eq!(runner::job_id(&first), job);
    assert_eq!(
        first["payload"],
        json!({ "form": "KEPT" }),
        "the trigger holds only the form"
    );
    assert!(
        !first.to_string().contains("the brief"),
        "the payload is not in the trigger"
    );
    pair.child.start_applied(&fixture, &first).await;
    assert_eq!(
        fetched_message(&fixture, &Work::of(&pair.child, &first)).await,
        "the brief"
    );

    pair.child
        .fail_run(&first, "TRANSIENT", "provider_unavailable", Some(0))
        .await;
    let retry = pair.child.next_trigger(LONG).await;
    assert_eq!(runner::attempt_number(&retry), 2);
    pair.child.start_applied(&fixture, &retry).await;
    let retry_work = Work::of(&pair.child, &retry);
    assert_eq!(
        fetched_message(&fixture, &retry_work).await,
        "the brief",
        "the retry fetches it"
    );

    let agent = Agent::with_inbox();
    let address = agent.agent_id;
    surface::register(fixture.nats(), &pair.child, retry_work.run_id, &[agent]).await;
    save_state(&fixture, &retry_work, json!({ "turn": 1 })).await;
    pair.child.complete_run(&retry).await;
    durable
        .await_count(FINISHED_OF_RUN, retry_work.run_id, 1, "the agent finished")
        .await;
    gesture::accepted(
        &gesture::send_mail(
            &fixture.gql(),
            &runner_access(),
            &caller.work,
            &Mail::new(caller.address, &[address]),
        )
        .await,
        "jobsSendMail",
    );
    let restart = pair.child.next_trigger(LONG).await;
    assert_ne!(runner::job_id(&restart), job, "the restart is another Job");
    pair.child.start_applied(&fixture, &restart).await;
    assert_eq!(
        fetched_message(&fixture, &Work::of(&pair.child, &restart)).await,
        "the brief",
        "the restart Job fetches the same payload"
    );

    let refused = upload(&fixture, &d, job, "a takeover").await;
    assert_eq!(
        gesture::refusal(&refused, "jobsUploadPayload"),
        "job_exists_not_yours"
    );

    let borrowed = Uuid::now_v7();
    gesture::accepted(
        &upload(&fixture, &caller, borrowed, "C's brief").await,
        "jobsUploadPayload",
    );
    create_kept(&fixture, &d, &pair.child.runner_type, borrowed).await;
    let rejected = answers
        .expect_one(wire::FACT_CREATION_REJECTED, borrowed, LONG)
        .await;
    assert_eq!(
        rejected.payload()["reason_code"],
        "payload_uploader_mismatch"
    );
    durable
        .assert_count(support::db::JOBS_WITH_ID, borrowed, 0, "no Job J2")
        .await;

    let too_big = JobDeclaration::new(&pair.child.runner_type);
    let mut body = too_big.payload("projects");
    body["payload"] =
        json!({ "form": "INLINE", "message": "x".repeat(600 * 1024), "attachments": [] });
    Producer::new(fixture.fabric(), "projects")
        .declare_payload(body)
        .await;
    let rejected = answers
        .expect_one(wire::FACT_CREATION_REJECTED, too_big.job_id, LONG)
        .await;
    assert_eq!(rejected.payload()["reason_code"], "payload_too_large");
    durable
        .assert_count(support::db::JOBS_WITH_ID, too_big.job_id, 0, "no Job")
        .await;
    answers.stop().await;
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_second_upload_for_a_job_id_keeps_the_first_payload_whoever_sends_it() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let mut pair = pair(&fixture, "kept-first").await;
    let caller = live_root(&fixture, &mut pair.parent).await;
    let mut d_runner = other_runner(&fixture, "kept-d").await;
    let d = live_root(&fixture, &mut d_runner).await;
    let job = Uuid::now_v7();

    gesture::accepted(
        &upload(&fixture, &caller, job, "the first").await,
        "jobsUploadPayload",
    );
    gesture::accepted(
        &upload(&fixture, &caller, job, "the first").await,
        "jobsUploadPayload",
    );
    durable
        .assert_count(KEPT_OF_JOB, job, 1, "J has one payload")
        .await;
    for (who, by) in [("C", &caller), ("D", &d)] {
        let refused = upload(&fixture, by, job, "another").await;
        assert_eq!(
            gesture::refusal(&refused, "jobsUploadPayload"),
            "payload_already_uploaded",
            "{who} uploading other content for J"
        );
        assert!(
            !gesture::field(&refused, "jobsUploadPayload")["message"]
                .as_str()
                .unwrap_or_default()
                .is_empty()
        );
    }
    durable
        .assert_count(KEPT_OF_JOB, job, 1, "still one payload")
        .await;

    create_kept(&fixture, &caller, &pair.child.runner_type, job).await;
    let first = pair.child.next_trigger(LONG).await;
    pair.child.start_applied(&fixture, &first).await;
    assert_eq!(
        fetched_message(&fixture, &Work::of(&pair.child, &first)).await,
        "the first"
    );
    pair.child
        .fail_run(&first, "TRANSIENT", "provider_unavailable", Some(0))
        .await;
    let retry = pair.child.next_trigger(LONG).await;
    pair.child.start_applied(&fixture, &retry).await;
    assert_eq!(
        fetched_message(&fixture, &Work::of(&pair.child, &retry)).await,
        "the first",
        "every retry gets the first payload"
    );
    durable.close().await;
    fixture.shutdown().await;
}
