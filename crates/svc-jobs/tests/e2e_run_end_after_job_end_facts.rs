mod support;

use support::db::{self, Durable};
use support::fixture::JobsFixture;
use support::producer::{JobDeclaration, Producer};
use support::runner::{self, FakeRunner};
use support::surface::{self, Agent};
use support::{LONG, QUIET, dispatch, gql, infra, wire};
use uuid::Uuid;

const EVENTS_OF_JOB: &str = "SELECT count(*) FROM domain_events WHERE aggregate_id = $1";

async fn live_and_finished<'a>(
    fixture: &'a JobsFixture,
    instance: &mut FakeRunner<'a>,
) -> (Uuid, serde_json::Value) {
    let producer = Producer::new(fixture.fabric(), "projects");
    let declaration = JobDeclaration::new(&instance.runner_type).with_max_attempts(3);
    producer.declare_v2(&declaration).await;
    let trigger = instance.next_trigger(LONG).await;
    instance.start_applied(fixture, &trigger).await;
    let registered = surface::register(
        fixture.nats(),
        instance,
        runner::run_id(&trigger),
        &[Agent::with_inbox()],
    )
    .await;
    assert_eq!(registered["accepted"], true, "{registered}");
    producer.finish_v2(declaration.job_id).await;
    gql::wait_for_status(
        &fixture.gql(),
        fixture.admin(),
        declaration.job_id,
        "COMPLETED",
        LONG,
    )
    .await;
    (declaration.job_id, trigger)
}

async fn acknowledged(fixture: &JobsFixture) {
    let settled = br_test_harness::wait_until(LONG, || async {
        infra::status_frames_unacknowledged(fixture.nats()).await == 0
    })
    .await;
    assert!(settled, "every status frame is acknowledged");
}

#[tokio::test]
async fn a_run_that_fails_after_its_job_was_finished_starts_no_retry_and_a_second_fact_is_discarded()
 {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let runner_type = wire::unique_runner_type("late-end");
    let mut instance =
        FakeRunner::new(fixture.nats(), &runner_type, "instance-a").declaring_capacity(2);
    instance.connect().await;
    dispatch::await_live_instances(&fixture.gql(), fixture.admin(), &runner_type, 1, LONG).await;

    let (a, a_trigger) = live_and_finished(&fixture, &mut instance).await;
    let a_run = runner::run_id(&a_trigger);
    instance.complete_run(&a_trigger).await;
    durable
        .await_count(
            db::FINISHED_AGENTS_OF_RUN,
            a_run,
            1,
            "A's agent is FINISHED",
        )
        .await;
    durable
        .assert_count(
            db::CURRENT_RUNS_OF_RUN,
            a_run,
            0,
            "I is no longer busy with it",
        )
        .await;

    let (b, b_trigger) = live_and_finished(&fixture, &mut instance).await;
    let b_run = runner::run_id(&b_trigger);
    instance
        .fail_run(&b_trigger, "PERMANENT", "provider_refused", None)
        .await;
    durable
        .await_count(db::TERMINALS_OF_RUN, b_run, 1, "B's Run is failed")
        .await;
    durable
        .await_count(
            db::AGENTS_OF_RUN,
            b_run,
            0,
            "its agents leave the directory",
        )
        .await;
    instance.expect_no_trigger(QUIET).await;
    durable
        .assert_count(db::RUNS_OF_JOB, b, 1, "no retry starts")
        .await;
    durable
        .assert_count(db::RETRY_SCHEDULES_OF_JOB, b, 0, "no retry is scheduled")
        .await;
    assert_eq!(
        gql::status_of(&fixture.gql(), fixture.admin(), b).await,
        "COMPLETED"
    );
    assert_eq!(
        gql::status_of(&fixture.gql(), fixture.admin(), a).await,
        "COMPLETED"
    );
    acknowledged(&fixture).await;

    let (a_events, b_events) = (
        durable.count(EVENTS_OF_JOB, a).await,
        durable.count(EVENTS_OF_JOB, b).await,
    );
    assert!(
        a_events > 0 && b_events > 0,
        "the history of both Jobs is stored"
    );
    instance
        .fail_run(&a_trigger, "PERMANENT", "late_fact", None)
        .await;
    instance.complete_run(&b_trigger).await;
    acknowledged(&fixture).await;
    durable
        .assert_count(
            EVENTS_OF_JOB,
            a,
            a_events,
            "the second fact of A is discarded",
        )
        .await;
    durable
        .assert_count(
            EVENTS_OF_JOB,
            b,
            b_events,
            "the second fact of B is discarded",
        )
        .await;
    durable
        .assert_count(
            db::FINISHED_AGENTS_OF_RUN,
            a_run,
            1,
            "A's agent stays FINISHED",
        )
        .await;
    durable
        .assert_count(db::AGENTS_OF_RUN, b_run, 0, "B's agents stay gone")
        .await;
    durable
        .assert_count(db::TERMINALS_OF_RUN, a_run, 1, "one end for A's Run")
        .await;
    instance.expect_no_trigger(QUIET).await;
    durable.close().await;
    fixture.shutdown().await;
}
