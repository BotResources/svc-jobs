mod support;

use support::db::{self, Durable};
use support::fixture::{JobsFixture, Knobs};
use support::producer::{JobDeclaration, Producer};
use support::runner::{self, FakeRunner};
use support::surface::{self, Agent};
use support::{LONG, dispatch, gql, infra, wire};

async fn started_run_with_agent<'a>(
    fixture: &'a JobsFixture,
    label: &str,
) -> (FakeRunner<'a>, JobDeclaration, serde_json::Value) {
    let client = fixture.gql();
    let producer = Producer::new(fixture.fabric(), "projects");
    let runner_type = wire::unique_runner_type(label);
    let mut instance = FakeRunner::new(fixture.nats(), &runner_type, "instance-a");
    instance.connect().await;
    dispatch::await_live_instances(&client, fixture.admin(), &runner_type, 1, LONG).await;
    let declaration = JobDeclaration::new(&runner_type).with_max_attempts(1);
    producer.declare_v2(&declaration).await;
    let trigger = instance.next_trigger(LONG).await;
    instance.start_applied(fixture, &trigger).await;
    let registered = surface::register(
        fixture.nats(),
        &instance,
        runner::run_id(&trigger),
        &[Agent::with_inbox()],
    )
    .await;
    assert_eq!(registered["accepted"], true, "{registered}");
    (instance, declaration, trigger)
}

#[tokio::test]
async fn finishing_a_job_leaves_its_live_run_and_the_directory_alone_until_the_run_ends_by_success()
{
    let fixture = JobsFixture::start().await;
    let producer = Producer::new(fixture.fabric(), "projects");
    let (instance, declaration, trigger) =
        started_run_with_agent(&fixture, "end-after-finish").await;
    let run = runner::run_id(&trigger);
    let durable = Durable::open(&fixture.app_url()).await;

    producer.finish_v2(declaration.job_id).await;
    gql::wait_for_status(
        &fixture.gql(),
        fixture.admin(),
        declaration.job_id,
        "COMPLETED",
        LONG,
    )
    .await;
    durable
        .assert_count(
            db::AGENTS_OF_RUN,
            run,
            1,
            "a finish changes nothing in the directory",
        )
        .await;
    durable
        .assert_count(db::TERMINALS_OF_RUN, run, 0, "the Run is still live")
        .await;
    durable
        .assert_count(
            db::CURRENT_RUNS_OF_RUN,
            run,
            1,
            "the Run still holds its capacity",
        )
        .await;

    instance.complete_run(&trigger).await;
    durable
        .await_count(db::TERMINALS_OF_RUN, run, 1, "the Run ends as usual")
        .await;
    durable
        .await_count(
            db::FINISHED_AGENTS_OF_RUN,
            run,
            1,
            "the agents become FINISHED",
        )
        .await;
    durable
        .assert_count(db::CURRENT_RUNS_OF_RUN, run, 0, "the capacity is released")
        .await;
    assert_eq!(
        gql::status_of(&fixture.gql(), fixture.admin(), declaration.job_id).await,
        "COMPLETED",
        "the Job does not change"
    );
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_permanent_failure_of_the_live_run_of_a_finished_job_ends_the_run_and_leaves_the_job() {
    let fixture = JobsFixture::start().await;
    let producer = Producer::new(fixture.fabric(), "projects");
    let (instance, declaration, trigger) =
        started_run_with_agent(&fixture, "end-after-failure").await;
    let run = runner::run_id(&trigger);
    let durable = Durable::open(&fixture.app_url()).await;

    producer.finish_v2(declaration.job_id).await;
    gql::wait_for_status(
        &fixture.gql(),
        fixture.admin(),
        declaration.job_id,
        "COMPLETED",
        LONG,
    )
    .await;
    durable
        .assert_count(
            db::AGENTS_OF_RUN,
            run,
            1,
            "a finish changes nothing in the directory",
        )
        .await;

    instance
        .fail_run(&trigger, "PERMANENT", "provider_refused", None)
        .await;
    durable
        .await_count(db::TERMINALS_OF_RUN, run, 1, "the Run ends as usual")
        .await;
    durable
        .await_count(
            db::AGENTS_OF_RUN,
            run,
            0,
            "a failed Run takes its agents with it",
        )
        .await;
    durable
        .assert_count(db::CURRENT_RUNS_OF_RUN, run, 0, "the capacity is released")
        .await;
    assert_eq!(
        gql::status_of(&fixture.gql(), fixture.admin(), declaration.job_id).await,
        "COMPLETED",
        "the Job does not change"
    );
    durable.close().await;
    fixture.shutdown().await;
}

async fn finished(fixture: &JobsFixture, declaration: &JobDeclaration) {
    Producer::new(fixture.fabric(), "projects")
        .finish_v2(declaration.job_id)
        .await;
    gql::wait_for_status(
        &fixture.gql(),
        fixture.admin(),
        declaration.job_id,
        "COMPLETED",
        LONG,
    )
    .await;
}

fn quick_overrun() -> Knobs {
    Knobs {
        run_max_duration_seconds: 3,
        backstop_interval_seconds: 1,
        ..Knobs::default()
    }
}

#[tokio::test]
async fn the_crash_of_the_runner_of_a_finished_job_fails_its_live_run_as_instance_lost() {
    let fixture = JobsFixture::start().await;
    let (mut instance, declaration, trigger) =
        started_run_with_agent(&fixture, "end-after-crash").await;
    let run = runner::run_id(&trigger);
    let durable = Durable::open(&fixture.app_url()).await;
    finished(&fixture, &declaration).await;

    instance.crash();
    tokio::time::sleep(infra::PRESENCE_TTL).await;

    durable
        .await_count(db::TERMINALS_OF_RUN, run, 1, "the lost Run is failed")
        .await;
    durable
        .await_count(db::CURRENT_RUNS_OF_RUN, run, 0, "its capacity is released")
        .await;
    durable
        .await_count(db::AGENTS_OF_RUN, run, 0, "its agents leave the directory")
        .await;
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_live_run_of_a_finished_job_that_overruns_is_failed_by_the_backstop() {
    let fixture = JobsFixture::start_with(quick_overrun()).await;
    let (_instance, declaration, trigger) =
        started_run_with_agent(&fixture, "end-after-overrun").await;
    let run = runner::run_id(&trigger);
    let durable = Durable::open(&fixture.app_url()).await;
    finished(&fixture, &declaration).await;

    durable
        .await_count(
            db::TERMINALS_OF_RUN,
            run,
            1,
            "the overrunning Run is failed",
        )
        .await;
    durable
        .await_count(db::CURRENT_RUNS_OF_RUN, run, 0, "its capacity is released")
        .await;
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_live_run_of_a_finished_and_deleted_job_that_overruns_is_still_failed() {
    let fixture = JobsFixture::start_with(quick_overrun()).await;
    let (_instance, declaration, trigger) =
        started_run_with_agent(&fixture, "end-after-delete").await;
    let run = runner::run_id(&trigger);
    let durable = Durable::open(&fixture.app_url()).await;
    finished(&fixture, &declaration).await;
    let deleted = gql::delete_job(&fixture.gql(), fixture.admin(), declaration.job_id).await;
    gql::expect_success(
        &deleted,
        wire::FIELD_DELETE_JOB,
        "an administrator deletes a finished Job whose Run still lives",
    );

    durable
        .await_count(
            db::TERMINALS_OF_RUN,
            run,
            1,
            "the overrunning Run is failed",
        )
        .await;
    durable
        .await_count(db::CURRENT_RUNS_OF_RUN, run, 0, "its capacity is released")
        .await;
    durable
        .await_count(db::AGENTS_OF_RUN, run, 0, "its agents leave the directory")
        .await;
    durable.close().await;
    fixture.shutdown().await;
}
