mod support;

use support::fixture::JobsFixture;
use support::producer::{JobDeclaration, Producer};
use support::runner::{self, FakeRunner};
use support::surface::{self, Agent};
use support::{LONG, dispatch, wire};

const RUNS: usize = 12;

#[tokio::test]
async fn a_registration_right_after_the_start_fact_finds_the_run_started() {
    let fixture = JobsFixture::start().await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let producer = Producer::new(fixture.fabric(), "projects");
    let runner_type = wire::unique_runner_type("catch-up");
    let mut instance =
        FakeRunner::new(fixture.nats(), &runner_type, "instance-a").declaring_capacity(RUNS as u32);
    instance.connect().await;
    dispatch::await_live_instances(&client, admin, &runner_type, 1, LONG).await;
    for _ in 0..RUNS {
        producer
            .declare_v2(&JobDeclaration::new(&runner_type))
            .await;
    }
    let mut triggers = Vec::new();
    for _ in 0..RUNS {
        triggers.push(instance.next_trigger(LONG).await);
    }

    for trigger in &triggers {
        instance.start_run(trigger).await;
        let reply = surface::register(
            fixture.nats(),
            &instance,
            runner::run_id(trigger),
            &[Agent::with_inbox()],
        )
        .await;
        assert_eq!(
            reply["accepted"], true,
            "the start fact was acknowledged, so the run has started: {reply}"
        );
    }
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_registration_for_a_run_that_never_started_is_still_refused() {
    let fixture = JobsFixture::start().await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let producer = Producer::new(fixture.fabric(), "projects");
    let runner_type = wire::unique_runner_type("catch-up-none");
    let mut instance = FakeRunner::new(fixture.nats(), &runner_type, "instance-a");
    instance.connect().await;
    dispatch::await_live_instances(&client, admin, &runner_type, 1, LONG).await;
    producer
        .declare_v2(&JobDeclaration::new(&runner_type))
        .await;
    let trigger = instance.next_trigger(LONG).await;

    let reply = surface::register(
        fixture.nats(),
        &instance,
        runner::run_id(&trigger),
        &[Agent::with_inbox()],
    )
    .await;

    assert_eq!(surface::reason(&reply), "run_not_started");
    fixture.shutdown().await;
}
