mod support;

use br_test_harness::SseSubscription;
use serde_json::{Value, json};
use support::db::{self, Durable};
use support::events::EventLog;
use support::fixture::{JobsFixture, Knobs};
use support::producer::{JobDeclaration, Producer};
use support::runner::{self, FakeRunner};
use support::{
    FLEET_CHANGED, JOB_CHANGED, JOBS_CHANGED, LONG, QUIET, SHORT, delta, gql, stream, subs, wire,
};

#[tokio::test]
async fn two_jobs_instances_dispatch_a_waiting_job_exactly_once() {
    // Given: two instances of the same binary over one database and one NATS topology
    let fixture = JobsFixture::start_cluster(2, Knobs::default()).await;
    let events = EventLog::open(fixture.fabric()).await;
    let durable = Durable::open(&fixture.app_url()).await;
    let admin = fixture.admin();
    let pods = [fixture.url_of(0).to_owned(), fixture.url_of(1).to_owned()];
    let runner_type = wire::unique_runner_type("shared");
    let idle_type = wire::unique_runner_type("unrelated");
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut instance = FakeRunner::new(fixture.nats(), &runner_type, "instance-a");

    let declaration = JobDeclaration::new(&runner_type).with_max_attempts(2);
    let job_id = declaration.job_id;
    producer.declare(&declaration).await;
    events.expect_one(wire::FACT_QUEUED, job_id, LONG).await;

    // Given: all three subscriptions established through both pods, before any presence
    let mut job_watches = Vec::new();
    let mut list_watches = Vec::new();
    let mut fleet_watches = Vec::new();
    for pod in &pods {
        let mut job_watch = SseSubscription::open(pod, admin, &subs::job_changed(job_id)).await;
        let snapshot = stream::snapshot(&mut job_watch, JOB_CHANGED, SHORT).await;
        assert_eq!(snapshot["job"]["status"], json!("PENDING"));
        assert_eq!(snapshot["job"]["attemptCount"], json!(0));
        delta::assert_active_affordances(&snapshot);
        job_watches.push(job_watch);

        let mut list_watch =
            SseSubscription::open(pod, admin, &subs::jobs_changed(&runner_type)).await;
        stream::snapshot(&mut list_watch, JOBS_CHANGED, SHORT).await;
        list_watches.push(list_watch);

        let mut fleet_watch =
            SseSubscription::open(pod, admin, &subs::fleet_changed(&runner_type)).await;
        let fleet = stream::snapshot(&mut fleet_watch, FLEET_CHANGED, SHORT).await;
        assert!(
            fleet["runnerTypes"].as_array().is_none_or(|types| types
                .iter()
                .all(|view| { view["runnerType"]["isAvailable"] == json!(false) })),
            "no instance of the type is live yet on either pod: {fleet}",
        );
        fleet_watches.push(fleet_watch);
    }
    let mut unrelated =
        SseSubscription::open(&pods[0], admin, &subs::jobs_changed(&idle_type)).await;
    stream::snapshot(&mut unrelated, JOBS_CHANGED, SHORT).await;
    instance.expect_no_trigger(QUIET).await;

    // When: one live instance of the runner type announces itself
    instance.connect().await;

    // Then: exactly one dispatch, whatever the number of coordinating pods
    let trigger = instance.next_trigger(LONG).await;
    assert_eq!(runner::attempt_number(&trigger), 1);
    assert_eq!(runner::job_id(&trigger), job_id);
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
    durable
        .assert_all(
            job_id,
            &[(db::RUNS_OF_JOB, 1, "one durable dispatch fact for the job")],
        )
        .await;

    // Then: both pods fan the same facts out to their own subscribers
    for (index, job_watch) in job_watches.iter_mut().enumerate() {
        let dispatched =
            stream::await_delta(job_watch, JOB_CHANGED, wire::EVT_RUN_DISPATCHED, LONG).await;
        let event = delta::event_of(&dispatched, wire::EVT_RUN_DISPATCHED, job_id);
        assert_eq!(
            (event["runId"].clone(), event["attemptNumber"].clone()),
            (json!(run.to_string()), json!(1)),
            "pod {index} must broadcast the same dispatch identity as its peer",
        );
        let projection = delta::projection(&dispatched, job_id, "IN_PROGRESS");
        assert_eq!(projection["runs"].as_array().map(Vec::len), Some(1));
        delta::assert_active_affordances(&dispatched);
    }
    for list_watch in list_watches.iter_mut() {
        let listed =
            stream::await_delta(list_watch, JOBS_CHANGED, wire::EVT_RUN_DISPATCHED, LONG).await;
        delta::assert_active_affordances(&delta::assert_upserted_summary(
            &listed,
            job_id,
            "IN_PROGRESS",
        ));
    }
    for fleet_watch in fleet_watches.iter_mut() {
        stream::await_fleet_event(fleet_watch, wire::KIND_TYPE_REGISTERED, LONG).await;
        let connected =
            stream::await_fleet_event(fleet_watch, wire::KIND_INSTANCE_CONNECTED, LONG).await;
        assert_eq!(connected["event"]["instanceKey"], json!("instance-a"));
        assert_eq!(
            delta::fleet_projection(&connected, &runner_type)["executingJobCount"],
            json!(0),
            "a dispatched run is not executing until its runner says started: {connected}",
        );
    }

    // Then: both pods answer the same reads
    let mut answers: Vec<(Value, Value)> = Vec::new();
    for pod in &pods {
        let client = br_test_harness::GraphqlClient::new(pod);
        answers.push((
            gql::job_view(&client, admin, job_id).await,
            json!(gql::fleet_of(&client, admin, &runner_type).await),
        ));
    }
    assert_eq!(
        answers[0], answers[1],
        "the job and fleet reads must carry equal identities, state, counts and affordances \
         through either pod",
    );

    // When: the runner starts the one run
    instance.start_run(&trigger).await;
    for job_watch in job_watches.iter_mut() {
        let started =
            stream::await_delta(job_watch, JOB_CHANGED, wire::EVT_RUN_STARTED, LONG).await;
        assert_eq!(
            delta::event_of(&started, wire::EVT_RUN_STARTED, job_id)["runId"],
            json!(run.to_string()),
        );
    }
    for fleet_watch in fleet_watches.iter_mut() {
        let executing =
            stream::await_fleet_event(fleet_watch, wire::KIND_JOB_BEGAN_EXECUTING, LONG).await;
        delta::assert_instance(
            &delta::assert_fleet(&executing, &runner_type, 0, 1, 1, 0),
            "instance-a",
            true,
            &[run],
            &instance.version,
        );
    }
    events
        .expect_exactly(wire::FACT_STARTED, job_id, 1, QUIET)
        .await;
    assert_eq!(
        instance.trigger_count().await,
        1,
        "neither pod dispatches a second run for a job already in flight",
    );

    unrelated
        .expect_silence("an unrelated window is never woken by this job", QUIET)
        .await;
    for pod in &pods {
        assert!(
            gql::ready(pod).await,
            "both service instances must stay ready throughout the assertion phase",
        );
    }

    durable.close().await;
    events.stop().await;
    fixture.shutdown().await;
}
