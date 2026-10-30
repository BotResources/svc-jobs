mod support;

use std::time::Duration;

use support::claim;
use support::fixture::{JobsFixture, Knobs};
use support::producer::{JobDeclaration, Producer};
use support::runner::{self, FakeRunner};
use support::{LONG, QUIET, dispatch, gql, wire};

const CLAIM_WAIT: Duration = Duration::from_secs(6);

fn claim_knobs() -> Knobs {
    Knobs {
        claim_max_wait_seconds: 6,
        claim_reservation_seconds: 10,
        ..Knobs::default()
    }
}

#[tokio::test]
async fn push_dispatch_waits_for_free_capacity_and_resumes_when_a_run_ends() {
    let fixture = JobsFixture::start().await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let producer = Producer::new(fixture.fabric(), "projects");
    let runner_type = wire::unique_runner_type("push-capacity");
    let mut instance =
        FakeRunner::new(fixture.nats(), &runner_type, "instance-a").declaring_capacity(2);
    instance.connect().await;
    dispatch::await_live_instances(&client, admin, &runner_type, 1, LONG).await;

    let jobs: Vec<JobDeclaration> = (0..3).map(|_| JobDeclaration::new(&runner_type)).collect();
    for declaration in &jobs {
        producer.declare_v2(declaration).await;
    }

    let first = instance.next_trigger(LONG).await;
    let second = instance.next_trigger(LONG).await;
    instance.expect_no_trigger(QUIET).await;
    let waiting = jobs
        .iter()
        .find(|declaration| {
            ![runner::job_id(&first), runner::job_id(&second)].contains(&declaration.job_id)
        })
        .expect("one job stays behind the full capacity");
    assert_eq!(
        gql::status_of(&client, admin, waiting.job_id).await,
        "PENDING",
        "a job that finds no free capacity waits in the ledger"
    );

    instance.start_run(&first).await;
    instance.expect_no_trigger(QUIET).await;
    instance.complete_run(&first).await;

    let third = instance.next_trigger(LONG).await;
    assert_eq!(runner::job_id(&third), waiting.job_id);

    instance.announce_capacity(3).await;
    let extra = JobDeclaration::new(&runner_type);
    producer.declare_v2(&extra).await;
    let fourth = instance.next_trigger(LONG).await;
    assert_eq!(
        runner::job_id(&fourth),
        extra.job_id,
        "a larger declared capacity is free capacity at once"
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn claim_reservation_waits_for_free_capacity_and_is_served_when_a_run_ends() {
    let fixture = JobsFixture::start_with(claim_knobs()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let producer = Producer::new(fixture.fabric(), "projects");
    let runner_type = wire::unique_runner_type("claim-capacity");
    let mut instance = FakeRunner::new(fixture.nats(), &runner_type, "instance-a");
    instance.connect().await;
    dispatch::await_live_instances(&client, admin, &runner_type, 1, LONG).await;
    dispatch::switch_to_claim(&client, admin, &runner_type).await;

    let first_job = JobDeclaration::new(&runner_type);
    let second_job = JobDeclaration::new(&runner_type);
    producer.declare_v2(&first_job).await;
    producer.declare_v2(&second_job).await;

    let first = claim::claim(fixture.nats(), &instance, CLAIM_WAIT)
        .await
        .expect_run("a claim on a type with a free slot");
    let held = claim::claim(fixture.nats(), &instance, Duration::from_secs(3)).await;
    held.expect_empty("a claim finds no free capacity while the only slot is taken");
    held.expect_held_for_the_wait(
        Duration::from_secs(3),
        "a claim with no free capacity is held for its wait",
    );
    instance.start_run(&first).await;

    let (answer, ()) = tokio::join!(claim::claim(fixture.nats(), &instance, CLAIM_WAIT), async {
        tokio::time::sleep(Duration::from_secs(1)).await;
        instance.complete_run(&first).await;
    });
    let second = answer.expect_run("a held claim is served the moment a run ends");
    answer.expect_answered_at_once(CLAIM_WAIT, "a claim served by the end of a run");
    assert_ne!(runner::job_id(&second), runner::job_id(&first));
    fixture.shutdown().await;
}
