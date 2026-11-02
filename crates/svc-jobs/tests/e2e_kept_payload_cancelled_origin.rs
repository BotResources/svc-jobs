mod support;

use serde_json::json;
use support::db::Durable;
use support::fixture::{JobsFixture, runner_access};
use support::gesture::{self, Mail, Work};
use support::producer::{JobDeclaration, Producer};
use support::restart::{FINISHED_OF_RUN, save_state};
use support::runner;
use support::surface::{self, Agent};
use support::tree::pair;
use support::{LONG, gql, wire};
use uuid::Uuid;

const KEPT_OF_JOB: &str = "SELECT count(*) FROM kept_payloads WHERE job_id = $1";

#[tokio::test]
async fn the_restart_job_of_a_cancelled_job_cannot_be_retried_by_hand_with_the_payload_of_its_origin()
 {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let mut pair = pair(&fixture, "kept-cancelled-origin").await;
    let producer = Producer::new(fixture.fabric(), "projects");
    let root = JobDeclaration::new(&pair.parent.runner_type).with_max_attempts(1);
    producer.declare_v2(&root).await;
    let parent_trigger = pair.parent.next_trigger(LONG).await;
    pair.parent.start_applied(&fixture, &parent_trigger).await;
    let sender = Agent::with_inbox();
    let sender_id = sender.agent_id;
    surface::register(
        fixture.nats(),
        &pair.parent,
        runner::run_id(&parent_trigger),
        &[sender],
    )
    .await;
    let parent_work = Work::of(&pair.parent, &parent_trigger);

    let origin = Uuid::now_v7();
    gesture::accepted(
        &gesture::upload_payload(
            &fixture.gql(),
            &runner_access(),
            origin,
            "the brief",
            Some(&parent_work),
        )
        .await,
        "jobsUploadPayload",
    );
    producer
        .send_v3(
            wire::VERB_CREATE,
            json!({
                "job_id": origin.to_string(),
                "runner_type": pair.child.runner_type,
                "producer": "projects",
                "parent_job_id": root.job_id.to_string(),
                "mode": "SYNC",
                "max_attempts": 1,
                "payload": { "form": "KEPT" },
                "acting_run_id": parent_work.run_id.to_string(),
                "acting_instance_key": parent_work.instance_key,
            }),
        )
        .await;
    let first = pair.child.next_trigger(LONG).await;
    assert_eq!(runner::job_id(&first), origin);
    pair.child.start_applied(&fixture, &first).await;
    let first_work = Work::of(&pair.child, &first);
    let agent = Agent::with_inbox();
    let address = agent.agent_id;
    surface::register(fixture.nats(), &pair.child, first_work.run_id, &[agent]).await;
    save_state(&fixture, &first_work, json!({ "turn": 1 })).await;
    pair.child.complete_run(&first).await;
    durable
        .await_count(FINISHED_OF_RUN, first_work.run_id, 1, "the agent finished")
        .await;

    gesture::accepted(
        &gesture::send_mail(
            &fixture.gql(),
            &runner_access(),
            &parent_work,
            &Mail::new(sender_id, &[address]),
        )
        .await,
        "jobsSendMail",
    );
    let restart = pair.child.next_trigger(LONG).await;
    let restart_job = runner::job_id(&restart);
    assert_ne!(restart_job, origin);
    pair.child.start_applied(&fixture, &restart).await;
    pair.child
        .fail_run(&restart, "PERMANENT", "provider_refused", None)
        .await;
    gql::wait_for_status(&fixture.gql(), fixture.admin(), restart_job, "FAILED", LONG).await;
    durable
        .assert_count(
            KEPT_OF_JOB,
            origin,
            1,
            "the origin keeps its payload while open",
        )
        .await;

    gql::cancel_job(&fixture.gql(), fixture.admin(), Uuid::now_v7(), origin).await;
    gql::wait_for_status(&fixture.gql(), fixture.admin(), origin, "CANCELLED", LONG).await;
    durable
        .assert_count(
            KEPT_OF_JOB,
            origin,
            0,
            "the cancel erased the payload of the origin",
        )
        .await;
    assert_eq!(
        gql::status_of(&fixture.gql(), fixture.admin(), restart_job).await,
        "FAILED",
        "the restart Job already ended and the cancel leaves it as it is"
    );

    let resolution =
        Uuid::parse_str(
            gql::job_view(&fixture.gql(), fixture.admin(), restart_job).await["job"]["resolution"]
                ["id"]
                .as_str()
                .expect("a failed job names its resolution"),
        )
        .unwrap();
    let successor = Uuid::now_v7();
    let retried = gql::manual_retry_job(
        &fixture.gql(),
        fixture.admin(),
        Uuid::now_v7(),
        restart_job,
        successor,
        resolution,
    )
    .await;
    assert!(
        retried.get("errors").is_none_or(|e| e.is_null()),
        "{retried}"
    );
    let retry = pair.child.next_trigger(LONG).await;
    assert_eq!(runner::job_id(&retry), successor);
    pair.child.start_applied(&fixture, &retry).await;
    let fetched = gesture::payload(
        &fixture.gql(),
        &runner_access(),
        &Work::of(&pair.child, &retry),
    )
    .await;
    assert_eq!(gesture::refusal(&fetched, "jobsPayload"), "no_kept_payload");
    durable.close().await;
    fixture.shutdown().await;
}
