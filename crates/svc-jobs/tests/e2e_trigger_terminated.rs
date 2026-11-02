mod support;

use serde_json::json;
use support::db::{self, Durable};
use support::events::EventLog;
use support::fixture::JobsFixture;
use support::producer::{JobDeclaration, Producer};
use support::runner::{self, FakeRunner};
use support::{LONG, QUIET, gql, wire};

#[tokio::test]
async fn a_trigger_terminated_before_its_start_fails_its_run_for_good() {
    let fixture = JobsFixture::start().await;
    let events = EventLog::open(fixture.fabric()).await;
    let durable = Durable::open(&fixture.app_url()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let runner_type = wire::unique_runner_type("terminating");
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut instance = FakeRunner::new(fixture.nats(), &runner_type, "instance-a");
    let mut consumer = instance.consume_triggers().await;
    instance.connect().await;

    let declaration = JobDeclaration::new(&runner_type).with_max_attempts(3);
    let job_id = declaration.job_id;
    producer.declare(&declaration).await;
    let taken = consumer.next(LONG).await;
    let run = runner::run_id(&taken.payload);
    durable
        .assert_count(
            db::TRIGGERS_OF_RUN,
            run,
            1,
            "the stream sequence of the trigger is stored with its run",
        )
        .await;

    taken.terminate().await;

    gql::wait_for_status(&client, admin, job_id, "FAILED", LONG).await;
    durable
        .assert_count(
            db::FAILURES_OF_RUN_FOR_REASON,
            run,
            1,
            "the run failed as permanent with the reason trigger_terminated",
        )
        .await;
    let failed = events.expect_one(wire::FACT_FAILED, job_id, LONG).await;
    assert_eq!(
        failed.payload()["failure_cause"],
        json!("TERMINAL_RUN_FAILURE")
    );
    assert_eq!(
        failed.payload()["failure_report"]["reason_code"],
        json!("trigger_terminated")
    );
    assert_eq!(
        failed.payload()["failure_report"]["kind"],
        json!("PERMANENT")
    );
    tokio::time::sleep(QUIET).await;
    durable
        .assert_all(
            job_id,
            &[
                (
                    db::RUNS_OF_JOB,
                    1,
                    "a permanent failure leaves no second run although attempts remain",
                ),
                (
                    db::RETRY_SCHEDULES_OF_JOB,
                    0,
                    "a permanent failure schedules no retry",
                ),
            ],
        )
        .await;
    assert_eq!(
        instance.trigger_count().await,
        1,
        "no second trigger is dispatched"
    );

    let next = JobDeclaration::new(&runner_type);
    producer.declare(&next).await;
    let freed = consumer.next(LONG).await;
    assert_eq!(runner::job_id(&freed.payload), next.job_id);
    freed.acknowledge().await;

    durable.close().await;
    events.stop().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_run_that_started_before_the_termination_is_left_alone() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let runner_type = wire::unique_runner_type("started-first");
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut instance = FakeRunner::new(fixture.nats(), &runner_type, "instance-a");
    let mut consumer = instance.consume_triggers().await;
    instance.connect().await;

    let declaration = JobDeclaration::new(&runner_type);
    let job_id = declaration.job_id;
    producer.declare(&declaration).await;
    let taken = consumer.next(LONG).await;
    let trigger = taken.payload.clone();
    let run = runner::run_id(&trigger);
    instance.start_applied(&fixture, &trigger).await;
    gql::wait_for_status(&client, admin, job_id, "IN_PROGRESS", LONG).await;

    taken.terminate().await;
    tokio::time::sleep(QUIET).await;

    assert_eq!(gql::status_of(&client, admin, job_id).await, "IN_PROGRESS");
    durable
        .assert_count(
            db::TERMINALS_OF_RUN,
            run,
            0,
            "a run that started is not failed by the late termination of its trigger",
        )
        .await;
    instance.complete_run(&trigger).await;
    durable
        .await_count(
            db::TERMINALS_OF_RUN,
            run,
            1,
            "the run still ends as its runner reports",
        )
        .await;
    assert_eq!(
        durable
            .count(
                "SELECT count(*) FROM run_terminals WHERE run_id = $1 AND kind = 'COMPLETED'",
                run,
            )
            .await,
        1
    );

    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_termination_that_names_no_run_changes_nothing_and_the_next_one_is_still_served() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let foreign_type = wire::unique_runner_type("foreign");
    let runner_type = wire::unique_runner_type("served");
    let producer = Producer::new(fixture.fabric(), "projects");
    let foreign = FakeRunner::new(fixture.nats(), &foreign_type, "instance-f");
    let mut foreign_consumer = foreign.consume_triggers().await;
    let mut instance = FakeRunner::new(fixture.nats(), &runner_type, "instance-a");
    let mut consumer = instance.consume_triggers().await;
    instance.connect().await;

    let waiting = JobDeclaration::new(&runner_type);
    let job_id = waiting.job_id;
    producer.declare(&waiting).await;
    let taken = consumer.next(LONG).await;
    let run = runner::run_id(&taken.payload);

    foreign.publish_trigger_the_service_never_sent().await;
    foreign_consumer.next(LONG).await.terminate().await;
    tokio::time::sleep(QUIET).await;
    assert_eq!(
        gql::status_of(&client, admin, job_id).await,
        "IN_PROGRESS",
        "the termination of a message that is no run's trigger fails nothing"
    );
    durable
        .assert_count(db::TERMINALS_OF_RUN, run, 0, "the pending run is untouched")
        .await;

    taken.terminate().await;
    gql::wait_for_status(&client, admin, job_id, "FAILED", LONG).await;

    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn two_pods_fail_the_run_of_a_terminated_trigger_once() {
    let fixture = JobsFixture::start_cluster(2, support::fixture::Knobs::default()).await;
    let events = EventLog::open(fixture.fabric()).await;
    let durable = Durable::open(&fixture.app_url()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let runner_type = wire::unique_runner_type("two-pods");
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut instance = FakeRunner::new(fixture.nats(), &runner_type, "instance-a");
    let mut consumer = instance.consume_triggers().await;
    instance.connect().await;

    let declaration = JobDeclaration::new(&runner_type);
    let job_id = declaration.job_id;
    producer.declare(&declaration).await;
    let taken = consumer.next(LONG).await;
    let run = runner::run_id(&taken.payload);

    taken.terminate().await;

    gql::wait_for_status(&client, admin, job_id, "FAILED", LONG).await;
    events
        .expect_exactly(wire::FACT_FAILED, job_id, 1, QUIET)
        .await;
    durable
        .assert_count(
            db::FAILURES_OF_RUN_FOR_REASON,
            run,
            1,
            "one pod handled the advisory",
        )
        .await;

    durable.close().await;
    events.stop().await;
    fixture.shutdown().await;
}
