mod support;

use br_test_harness::SseSubscription;
use serde_json::json;
use support::events::EventLog;
use support::fixture::{JobsFixture, Knobs};
use support::producer::{JobDeclaration, Producer};
use support::runner::{self, FakeRunner};
use support::{
    FLEET_CHANGED, JOB_CHANGED, LONG, QUIET, SHORT, delta, gql, infra, stream, subs, wire,
};

#[tokio::test]
async fn an_unreadable_entry_from_a_live_instance_drains_it_instead_of_leaving_it_dispatchable() {
    // Given: an instance executing a run, watched from the fleet
    let fixture = JobsFixture::start_with(Knobs::default()).await;
    let admin = fixture.admin();
    let runner_type = wire::unique_runner_type("garbling");
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut instance = FakeRunner::new(fixture.nats(), &runner_type, "instance-a");
    let announced_version = instance.version.clone();
    instance.connect().await;

    let mut fleet_watch =
        SseSubscription::open(fixture.url(), admin, &subs::fleet_changed(&runner_type)).await;
    stream::snapshot(&mut fleet_watch, FLEET_CHANGED, SHORT).await;
    stream::await_fleet_event(&mut fleet_watch, wire::KIND_INSTANCE_CONNECTED, LONG).await;

    let declaration = JobDeclaration::new(&runner_type).with_max_attempts(2);
    let job_id = declaration.job_id;
    producer.declare(&declaration).await;
    let trigger = instance.next_trigger(LONG).await;
    let claimed_run = runner::run_id(&trigger);
    instance.start_run(&trigger).await;
    stream::await_fleet_event(&mut fleet_watch, wire::KIND_JOB_BEGAN_EXECUTING, LONG).await;

    // When: the instance stops heartbeating and its entry is rewritten in a form nobody can read
    instance.crash();
    instance.write_unreadable_presence().await;

    // Then: it is recorded as draining — its session and its run survive, but nothing new goes out
    let drained =
        stream::await_fleet_event(&mut fleet_watch, wire::KIND_INSTANCE_STATUS_REPORTED, LONG)
            .await;
    let projection = delta::fleet_projection(&drained, &runner_type);
    assert_eq!(
        projection["instances"][0]["reportedStatus"],
        json!(wire::STATUS_DRAINING),
        "an entry this service cannot read is never taken for READY, and a live instance is never \
         silently left dispatchable on the last report it managed to write: {drained}",
    );
    assert_eq!(projection["isAvailable"], json!(false));
    assert_eq!(
        gql::assert_blocked(&drained, wire::ACTION_DISPATCH),
        wire::REASON_RUNNER_TYPE_UNAVAILABLE,
    );
    delta::assert_instance(
        &projection,
        "instance-a",
        true,
        &[claimed_run],
        &announced_version,
    );

    // Then: a job declared while the fleet is mute waits instead of being handed to nobody
    let waiting = JobDeclaration::new(&runner_type).with_max_attempts(1);
    producer.declare(&waiting).await;
    instance.expect_no_trigger(QUIET).await;
    assert_eq!(
        gql::status_of(&fixture.gql(), admin, job_id).await,
        "IN_PROGRESS",
        "the run the drained instance already carries is untouched",
    );

    fixture.shutdown().await;
}

#[tokio::test]
async fn a_presence_session_whose_entry_vanished_unnoticed_is_settled_by_the_backstop() {
    // Given: a one-attempt job executing on a live instance
    let fixture = JobsFixture::start_with(Knobs {
        backstop_interval_seconds: 1,
        ..Knobs::default()
    })
    .await;
    let events = EventLog::open(fixture.fabric()).await;
    let admin = fixture.admin();
    let runner_type = wire::unique_runner_type("stranded");
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut instance = FakeRunner::new(fixture.nats(), &runner_type, "instance-a");
    instance.connect().await;

    let mut fleet_watch =
        SseSubscription::open(fixture.url(), admin, &subs::fleet_changed(&runner_type)).await;
    stream::snapshot(&mut fleet_watch, FLEET_CHANGED, SHORT).await;
    stream::await_fleet_event(&mut fleet_watch, wire::KIND_INSTANCE_CONNECTED, LONG).await;

    let declaration = JobDeclaration::new(&runner_type).with_max_attempts(1);
    let job_id = declaration.job_id;
    producer.declare(&declaration).await;
    events.expect_one(wire::FACT_QUEUED, job_id, LONG).await;
    let mut job_watch =
        SseSubscription::open(fixture.url(), admin, &subs::job_changed(job_id)).await;
    stream::snapshot(&mut job_watch, JOB_CHANGED, SHORT).await;
    let trigger = instance.next_trigger(LONG).await;
    let claimed_run = runner::run_id(&trigger);
    instance.start_run(&trigger).await;
    let started =
        stream::await_delta(&mut job_watch, JOB_CHANGED, wire::EVT_RUN_STARTED, LONG).await;
    assert_eq!(
        delta::event_of(&started, wire::EVT_RUN_STARTED, job_id)["instanceKey"],
        json!("instance-a"),
    );

    // When: the entry leaves the bucket with no signal this service could ever have seen —
    // the eviction lands while the pod is down, or its record of the loss was abandoned under
    // contention. Either way the session stays open in the database and the process is gone.
    instance.crash();
    infra::evict_presence_entry_unnoticed(fixture.nats(), &runner_type, "instance-a").await;
    assert!(
        instance.presence_entry().await.is_none(),
        "the precondition of this scenario is an instance the bucket no longer knows about",
    );

    // Then: the backstop reconciles the two, closes the session and reclaims what it carried
    let disconnected =
        stream::await_fleet_event(&mut fleet_watch, wire::KIND_INSTANCE_DISCONNECTED, LONG).await;
    assert_eq!(
        disconnected["event"]["instanceKey"],
        json!("instance-a"),
        "an open session no entry backs is a loss nobody recorded; the sweep is the only thing \
         that will ever notice it: {disconnected}",
    );
    let emptied = delta::fleet_projection(&disconnected, &runner_type);
    assert_eq!(emptied["instances"], json!([]));
    assert_eq!(emptied["isAvailable"], json!(false));

    let run_failed =
        stream::await_delta(&mut job_watch, JOB_CHANGED, wire::EVT_RUN_FAILED, LONG).await;
    let orphaned = delta::event_of(&run_failed, wire::EVT_RUN_FAILED, job_id);
    assert_eq!(orphaned["runId"], json!(claimed_run.to_string()));
    assert_eq!(
        orphaned["reasonCode"],
        json!(wire::REASON_INSTANCE_LOST),
        "a run reclaimed by the sweep is failed under the same reason as one the watch reclaimed",
    );
    let job_failed =
        stream::await_delta(&mut job_watch, JOB_CHANGED, wire::EVT_JOB_FAILED, LONG).await;
    delta::projection(&job_failed, job_id, "FAILED");
    assert_eq!(
        events
            .expect_one(wire::FACT_FAILED, job_id, LONG)
            .await
            .payload()["failure_cause"],
        json!("TERMINAL_RUN_FAILURE"),
        "the owner learns its work is dead — a job stranded on a session nobody closed would \
         wait for ever",
    );

    events.stop().await;
    fixture.shutdown().await;
}
