mod support;

use serde_json::{Value, json};
use support::db::{self, Durable};
use support::events::EventLog;
use support::fixture::JobsFixture;
use support::producer::{JobDeclaration, Producer};
use support::{LONG, QUIET, wire};
use uuid::Uuid;

fn bare_root(runner_type: &str, producer: &str) -> (Uuid, Value) {
    let job_id = Uuid::now_v7();
    (
        job_id,
        json!({ "job_id": job_id.to_string(), "runner_type": runner_type, "producer": producer }),
    )
}

#[tokio::test]
async fn a_root_creation_without_a_passport_is_refused_in_every_contract_version() {
    let fixture = JobsFixture::start().await;
    let events = EventLog::open(fixture.fabric()).await;
    let answers = EventLog::open_v2(fixture.fabric()).await;
    let durable = Durable::open(&fixture.app_url()).await;
    let producer = Producer::new(fixture.fabric(), "projects");
    let runner_type = wire::unique_runner_type("passportless");

    let (by_v3, payload_v3) = bare_root(&runner_type, "projects");
    let command = producer.send_v3(wire::VERB_CREATE, payload_v3).await;
    let (by_v1, payload_v1) = bare_root(&runner_type, "projects");
    producer.send_v1(wire::VERB_CREATE, payload_v1).await;

    let rejected = answers
        .expect_one(wire::FACT_CREATION_REJECTED, by_v3, LONG)
        .await;
    let payload = rejected.payload();
    assert_eq!(payload["reason_code"], "passport_required");
    assert_eq!(payload["parent_job_id"], by_v3.to_string());
    assert_eq!(payload["initial_job_id"], by_v3.to_string());
    assert!(
        payload["message"]
            .as_str()
            .is_some_and(|message| !message.is_empty()),
        "a refusal says why in plain words",
    );
    assert_eq!(
        rejected.correlation_id(),
        command.metadata.correlation_id.to_string()
    );
    assert_eq!(
        rejected.envelope["metadata"]["causation_id"],
        command.command_id.to_string()
    );
    let by_v1_rejected = events
        .expect_one(wire::FACT_CREATION_REJECTED, by_v1, LONG)
        .await;
    assert_eq!(by_v1_rejected.payload()["reason_code"], "passport_required");
    for refused in [by_v3, by_v1] {
        durable
            .assert_count(db::JOBS_WITH_ID, refused, 0, "a refused root leaves no job")
            .await;
        answers.expect_none(wire::FACT_QUEUED, refused, QUIET).await;
    }
    durable.close().await;
    events.stop().await;
    answers.stop().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_root_with_its_passport_is_queued_as_its_own_parent_and_initial_job() {
    let fixture = JobsFixture::start().await;
    let answers = EventLog::open_v2(fixture.fabric()).await;
    let events = EventLog::open(fixture.fabric()).await;
    let durable = Durable::open(&fixture.app_url()).await;
    let producer = Producer::new(fixture.fabric(), "projects");
    let runner_type = wire::unique_runner_type("rooted");

    let declaration = JobDeclaration::new(&runner_type);
    let job_id = declaration.job_id;
    let command = producer.declare(&declaration).await;

    let queued = answers.expect_one(wire::FACT_QUEUED, job_id, LONG).await;
    let payload = queued.payload();
    assert_eq!(payload["parent_job_id"], job_id.to_string());
    assert_eq!(payload["initial_job_id"], job_id.to_string());
    assert_eq!(payload["requester"]["kind"], "SERVICE");
    assert_eq!(payload["requester"]["service_key"], "projects");
    assert_eq!(
        queued.correlation_id(),
        command.metadata.correlation_id.to_string()
    );
    events.expect_one(wire::FACT_QUEUED, job_id, LONG).await;
    durable
        .assert_count(
            "SELECT count(*) FROM jobs WHERE id = $1 AND passport IS NOT NULL AND requester IS NOT NULL",
            job_id,
            1,
            "the Passport and the Requester are stored with the Job",
        )
        .await;
    durable.close().await;
    events.stop().await;
    answers.stop().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_child_takes_the_passport_of_its_parent_and_a_different_one_is_refused() {
    let fixture = JobsFixture::start().await;
    let answers = EventLog::open_v2(fixture.fabric()).await;
    let producer = Producer::new(fixture.fabric(), "projects");
    let runner_type = wire::unique_runner_type("lineage");
    let root = JobDeclaration::new(&runner_type);
    producer.declare(&root).await;
    answers
        .expect_one(wire::FACT_QUEUED, root.job_id, LONG)
        .await;

    let child = JobDeclaration::new(&runner_type).with_parent(root.job_id);
    producer.declare(&child).await;
    let mut foreign = child_payload(&runner_type, root.job_id);
    let foreign_id = foreign["job_id"]
        .as_str()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or_default();
    foreign["passport"] = JobDeclaration::new(&runner_type)
        .triggered_by_user(Uuid::now_v7())
        .payload("projects")["passport"]
        .clone();
    producer.send_v3(wire::VERB_CREATE, foreign).await;

    let queued = answers
        .expect_one(wire::FACT_QUEUED, child.job_id, LONG)
        .await;
    assert_eq!(queued.payload()["parent_job_id"], root.job_id.to_string());
    assert_eq!(queued.payload()["initial_job_id"], root.job_id.to_string());
    let rejected = answers
        .expect_one(wire::FACT_CREATION_REJECTED, foreign_id, LONG)
        .await;
    assert_eq!(
        rejected.payload()["reason_code"],
        "passport_parent_mismatch"
    );
    answers.stop().await;
    fixture.shutdown().await;
}

fn child_payload(runner_type: &str, parent: Uuid) -> Value {
    json!({
        "job_id": Uuid::now_v7().to_string(),
        "runner_type": runner_type,
        "producer": "projects",
        "parent_job_id": parent.to_string(),
    })
}

#[tokio::test]
async fn only_the_requester_finishes_fails_or_cancels_and_a_refusal_names_its_reason() {
    let fixture = JobsFixture::start().await;
    let answers = EventLog::open_v2(fixture.fabric()).await;
    let requester = Producer::new(fixture.fabric(), "projects");
    let stranger = Producer::new(fixture.fabric(), "billing");
    let runner_type = wire::unique_runner_type("requested");
    let declaration = JobDeclaration::new(&runner_type);
    let job_id = declaration.job_id;
    requester.declare(&declaration).await;
    answers.expect_one(wire::FACT_QUEUED, job_id, LONG).await;

    let body = json!({ "job_id": job_id.to_string() });
    let finish = stranger.send_v3(wire::VERB_FINISH, body.clone()).await;
    stranger.send_v3(wire::VERB_FAIL, body.clone()).await;
    stranger.send_v3(wire::VERB_CANCEL, body.clone()).await;

    let refusals = answers
        .expect_exactly(wire::FACT_COMMAND_REJECTED, job_id, 3, LONG)
        .await;
    let mut kinds: Vec<String> = refusals
        .iter()
        .map(|refusal| {
            refusal.payload()["command_type"]
                .as_str()
                .unwrap_or_default()
                .to_owned()
        })
        .collect();
    kinds.sort();
    assert_eq!(kinds, vec!["job.cancel", "job.fail", "job.finish"]);
    for refusal in &refusals {
        assert_eq!(refusal.payload()["reason_code"], "not_requester");
        assert_eq!(refusal.payload()["parent_job_id"], job_id.to_string());
    }
    assert!(
        refusals
            .iter()
            .any(|refusal| refusal.correlation_id() == finish.metadata.correlation_id.to_string()),
        "a refusal copies the correlation id of its command",
    );
    answers
        .expect_none(wire::FACT_COMPLETED, job_id, QUIET)
        .await;
    answers
        .expect_none(wire::FACT_CANCELLED, job_id, QUIET)
        .await;

    requester.send_v3(wire::VERB_FINISH, body.clone()).await;

    let completed = answers.expect_one(wire::FACT_COMPLETED, job_id, LONG).await;
    assert_eq!(completed.payload()["initial_job_id"], job_id.to_string());
    requester.send_v3(wire::VERB_FINISH, body).await;
    let refusals = answers
        .expect_exactly(wire::FACT_COMMAND_REJECTED, job_id, 4, LONG)
        .await;
    assert!(
        refusals
            .iter()
            .any(|refusal| refusal.payload()["reason_code"] == "job_terminal"),
        "a finish of a terminal Job says job_terminal",
    );
    answers.stop().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn the_requester_cancels_a_job_and_is_told_it_was_accepted_even_when_it_is_already_over() {
    let fixture = JobsFixture::start().await;
    let answers = EventLog::open_v2(fixture.fabric()).await;
    let requester = Producer::new(fixture.fabric(), "projects");
    let runner_type = wire::unique_runner_type("cancelled");
    let declaration = JobDeclaration::new(&runner_type);
    let job_id = declaration.job_id;
    requester.declare(&declaration).await;
    answers.expect_one(wire::FACT_QUEUED, job_id, LONG).await;
    let body = json!({ "job_id": job_id.to_string() });

    let first = requester.send_v3(wire::VERB_CANCEL, body.clone()).await;
    answers.expect_one(wire::FACT_CANCELLED, job_id, LONG).await;
    requester.send_v3(wire::VERB_CANCEL, body).await;

    let accepted = answers
        .expect_exactly(wire::FACT_CANCEL_ACCEPTED, job_id, 2, LONG)
        .await;
    assert!(
        accepted
            .iter()
            .any(|event| event.correlation_id() == first.metadata.correlation_id.to_string()),
        "the acceptance copies the correlation id of its command",
    );
    answers
        .expect_exactly(wire::FACT_CANCELLED, job_id, 1, QUIET)
        .await;
    answers.stop().await;
    fixture.shutdown().await;
}
