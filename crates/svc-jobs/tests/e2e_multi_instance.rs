mod support;

use br_test_harness::SseSubscription;
use serde_json::json;
use support::events::EventLog;
use support::fixture::{JobsFixture, Knobs};
use support::producer::{JobDeclaration, Producer};
use support::runner::{self, FakeRunner};
use support::{LONG, QUIET, SHORT, docs, gql, stream, wire};
use uuid::Uuid;

const JOB_CHANGED: &str = "jobsJobChanged";

#[tokio::test]
async fn two_jobs_instances_dispatch_a_waiting_job_exactly_once() {
    let fixture = JobsFixture::start_cluster(2, Knobs::default()).await;
    let events = EventLog::open(fixture.fabric()).await;
    let first_pod = fixture.gql_of(0);
    let second_pod = fixture.gql_of(1);
    let admin = fixture.admin();
    let runner_type = wire::unique_runner_type("shared");
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut instance = FakeRunner::new(fixture.nats(), &runner_type, "instance-a");

    let declaration = JobDeclaration::new(&runner_type).with_max_attempts(2);
    let job_id = declaration.job_id;
    producer.declare(&declaration).await;
    events.expect_one(wire::FACT_QUEUED, job_id, LONG).await;

    for pod in [&first_pod, &second_pod] {
        assert_eq!(
            gql::status_of(pod, admin, job_id).await,
            "PENDING",
            "both instances read the same waiting job",
        );
    }
    instance.expect_no_trigger(QUIET).await;

    let mut watch = SseSubscription::open(
        fixture.url_of(1),
        admin,
        &docs::job_changed_subscription(job_id),
    )
    .await;
    stream::snapshot(&mut watch, JOB_CHANGED, SHORT).await;

    instance.connect().await;

    let trigger = instance.next_trigger(LONG).await;
    assert_eq!(runner::attempt_number(&trigger), 1);
    let run = runner::run_id(&trigger);
    instance.expect_no_trigger(QUIET).await;
    assert_eq!(
        instance.trigger_count().await,
        1,
        "concurrent instances dispatch the waiting job exactly once",
    );
    events
        .expect_exactly(wire::FACT_QUEUED, job_id, 1, QUIET)
        .await;

    for pod in [&first_pod, &second_pod] {
        let job = gql::job(pod, admin, job_id).await;
        assert_eq!(
            job["attemptCount"],
            json!(1),
            "no divergent state between pods"
        );
        assert_eq!(job["activeRunId"], json!(run.to_string()));
        assert_eq!(job["runs"].as_array().map(Vec::len), Some(1));
    }

    instance.start_run(&trigger).await;
    stream::await_delta(&mut watch, JOB_CHANGED, "JobsRunStartedEvent", LONG).await;
    events
        .expect_exactly(wire::FACT_STARTED, job_id, 1, QUIET)
        .await;

    instance.complete_run(&trigger).await;
    producer.finish(job_id, Uuid::now_v7()).await;
    stream::await_delta(&mut watch, JOB_CHANGED, "JobsJobCompletedEvent", LONG).await;

    for pod in [&first_pod, &second_pod] {
        gql::wait_for_status(pod, admin, job_id, "COMPLETED", LONG).await;
        let view = gql::job_view(pod, admin, job_id).await;
        assert_eq!(view["job"]["attemptCount"], json!(1));
        gql::assert_blocked(&view, wire::ACTION_CANCEL);
        gql::assert_allowed(&view, wire::ACTION_DELETE);
    }
    events
        .expect_exactly(wire::FACT_COMPLETED, job_id, 1, QUIET)
        .await;
    assert_eq!(
        instance.trigger_count().await,
        1,
        "a completed job is never dispatched again by the other instance",
    );

    events.stop().await;
    fixture.shutdown().await;
}
