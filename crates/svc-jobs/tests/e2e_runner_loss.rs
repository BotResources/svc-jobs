mod support;

use br_test_harness::SseSubscription;
use serde_json::json;
use support::events::EventLog;
use support::fixture::JobsFixture;
use support::producer::{JobDeclaration, Producer};
use support::runner::{self, FakeRunner};
use support::{LONG, QUIET, SHORT, docs, gql, stream, wire};
use uuid::Uuid;

const JOB_CHANGED: &str = "jobsJobChanged";
const FLEET_CHANGED: &str = "jobsFleetChanged";

#[tokio::test]
async fn a_job_survives_the_loss_of_its_runner_without_administrator_intervention() {
    let fixture = JobsFixture::start().await;
    let events = EventLog::open(fixture.fabric()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let runner_type = wire::unique_runner_type("volatile");
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut lost = FakeRunner::new(fixture.nats(), &runner_type, "instance-lost");
    let mut replacement = FakeRunner::new(fixture.nats(), &runner_type, "instance-replacement");

    let declaration = JobDeclaration::new(&runner_type).with_max_attempts(3);
    let job_id = declaration.job_id;
    producer.declare(&declaration).await;
    events.expect_one(wire::FACT_QUEUED, job_id, LONG).await;

    let mut job_watch = SseSubscription::open(
        fixture.url(),
        admin,
        &docs::job_changed_subscription(job_id),
    )
    .await;
    stream::snapshot(&mut job_watch, JOB_CHANGED, SHORT).await;
    let mut fleet_watch = SseSubscription::open(
        fixture.url(),
        admin,
        &docs::fleet_changed_subscription(&runner_type),
    )
    .await;
    stream::snapshot(&mut fleet_watch, FLEET_CHANGED, SHORT).await;

    lost.connect().await;
    stream::await_message(
        &mut fleet_watch,
        FLEET_CHANGED,
        "INSTANCE_CONNECTED",
        |message| message["event"]["kind"] == json!("INSTANCE_CONNECTED"),
        LONG,
    )
    .await;

    let first = lost.next_trigger(LONG).await;
    let first_run = runner::run_id(&first);
    lost.start_run(&first).await;
    gql::wait_for_status(&client, admin, job_id, "IN_PROGRESS", LONG).await;
    let live = gql::fleet_of(&client, admin, &runner_type).await;
    assert_eq!(live[0]["runnerType"]["isAvailable"], json!(true));
    assert_eq!(live[0]["runnerType"]["executingJobCount"], json!(1));

    lost.disconnect().await;

    let disconnected = stream::await_message(
        &mut fleet_watch,
        FLEET_CHANGED,
        "INSTANCE_DISCONNECTED",
        |message| message["event"]["kind"] == json!("INSTANCE_DISCONNECTED"),
        LONG,
    )
    .await;
    assert_eq!(
        disconnected["event"]["instanceKey"],
        json!("instance-lost"),
        "graceful removal of the presence entry is the disconnection signal",
    );
    assert_eq!(disconnected["runnerType"]["isAvailable"], json!(false));

    stream::await_delta(&mut job_watch, JOB_CHANGED, "JobsRunFailedEvent", LONG).await;
    let after_loss = gql::job(&client, admin, job_id).await;
    let orphaned = gql::run_by_id(&after_loss, first_run);
    assert_eq!(orphaned["status"], json!("FAILED"));
    assert_eq!(orphaned["failureReport"]["kind"], json!("TRANSIENT"));
    assert_eq!(
        orphaned["failureReport"]["reasonCode"],
        json!("instance_lost"),
        "losing an instance fails its runs as transient under the reason the spec names",
    );
    assert_eq!(
        after_loss["status"],
        json!("IN_PROGRESS"),
        "the job survives the loss of its runner",
    );
    assert!(!after_loss["nextAttemptAt"].is_null());
    events.expect_none(wire::FACT_FAILED, job_id, QUIET).await;
    let surviving = gql::job_view(&client, admin, job_id).await;
    gql::assert_allowed(&surviving, wire::ACTION_CANCEL);
    gql::assert_blocked(&surviving, wire::ACTION_MANUAL_RETRY);

    let empty = gql::fleet_of(&client, admin, &runner_type).await;
    assert_eq!(empty[0]["runnerType"]["instances"], json!([]));
    assert_eq!(empty[0]["runnerType"]["isAvailable"], json!(false));
    replacement.expect_no_trigger(QUIET).await;
    assert_eq!(
        gql::status_of(&client, admin, job_id).await,
        "IN_PROGRESS",
        "no administrator touched this job",
    );

    replacement.connect().await;
    let second = replacement.next_trigger(LONG).await;
    assert_eq!(runner::attempt_number(&second), 2);
    assert_eq!(runner::job_id(&second), job_id);
    replacement.start_run(&second).await;
    events
        .expect_exactly(wire::FACT_STARTED, job_id, 1, QUIET)
        .await;

    let recovered = gql::job(&client, admin, job_id).await;
    let retried = gql::run_by_id(&recovered, runner::run_id(&second));
    assert_eq!(retried["origin"], json!("AUTOMATIC_RETRY"));
    assert_eq!(
        retried["instance"]["instanceKey"],
        json!("instance-replacement")
    );
    assert_eq!(recovered["attemptCount"], json!(2));

    replacement.complete_run(&second).await;
    producer.finish(job_id, Uuid::now_v7()).await;
    gql::wait_for_status(&client, admin, job_id, "COMPLETED", LONG).await;
    events.expect_one(wire::FACT_COMPLETED, job_id, LONG).await;
    events.expect_none(wire::FACT_FAILED, job_id, QUIET).await;

    events.stop().await;
    fixture.shutdown().await;
}
