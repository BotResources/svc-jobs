mod support;

use std::time::Duration;

use br_core_auth::Passport;
use br_test_harness::GraphqlClient;
use serde_json::json;
use support::claim;
use support::fixture::{JobsFixture, Knobs};
use support::infra;
use support::producer::{JobDeclaration, Producer};
use support::runner::{self, FakeRunner};
use support::{LONG, QUIET, dispatch, gql, wire};

const CLAIM_WAIT: Duration = Duration::from_secs(6);
const TOTAL_CAPACITY: &str = "query($runnerType: String) { jobsFleet(runnerType: $runnerType) { runnerType { totalCapacity } } }";

async fn await_total_capacity(
    client: &GraphqlClient,
    admin: &Passport,
    runner_type: &str,
    expected: i64,
) {
    let reached = br_test_harness::wait_until(LONG, || async {
        let response = client
            .query(admin, TOTAL_CAPACITY, json!({ "runnerType": runner_type }))
            .await;
        response["data"]["jobsFleet"][0]["runnerType"]["totalCapacity"] == json!(expected)
    })
    .await;
    assert!(
        reached,
        "the total capacity of {runner_type} reaches {expected}"
    );
}

#[tokio::test]
async fn a_capacity_lowered_by_a_heartbeat_holds_the_claim_until_a_live_run_ends() {
    let fixture = JobsFixture::start_with(Knobs {
        claim_max_wait_seconds: 6,
        claim_reservation_seconds: 10,
        ..Knobs::default()
    })
    .await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let producer = Producer::new(fixture.fabric(), "projects");
    let runner_type = wire::unique_runner_type("claim-lowered");
    let mut instance =
        FakeRunner::new(fixture.nats(), &runner_type, "instance-a").declaring_capacity(2);
    instance.connect().await;
    dispatch::await_live_instances(&client, admin, &runner_type, 1, LONG).await;
    dispatch::switch_to_claim(&client, admin, &runner_type).await;
    await_total_capacity(&client, admin, &runner_type, 2).await;

    producer
        .declare_v2(&JobDeclaration::new(&runner_type))
        .await;
    let (one, other) = tokio::join!(
        claim::claim(fixture.nats(), &instance, Duration::from_secs(2)),
        claim::claim(fixture.nats(), &instance, Duration::from_secs(2)),
    );
    let (served, held) = if !one.reply["run"].is_null() {
        (one, other)
    } else {
        (other, one)
    };
    let first = served.expect_run("one of the two held claims is served with the one waiting Job");
    held.expect_empty("the other claim stays held: no second Job waits");
    instance.start_run(&first).await;

    let (unserved, ()) = tokio::join!(claim::claim(fixture.nats(), &instance, CLAIM_WAIT), async {
        tokio::time::sleep(Duration::from_millis(500)).await;
        instance.announce_capacity(1).await;
        await_total_capacity(&client, admin, &runner_type, 1).await;
        producer
            .declare_v2(&JobDeclaration::new(&runner_type))
            .await;
    });
    unserved.expect_empty("the held claim is not served although a Job waits: no free capacity");
    unserved.expect_held_for_the_wait(
        CLAIM_WAIT,
        "and it is answered empty at the end of its wait",
    );
    assert_eq!(
        dispatch::runner_type_view(&client, admin, &runner_type).await["runnerType"]["waitingJobCount"],
        1,
        "the second Job waits"
    );

    let (served, ()) = tokio::join!(claim::claim(fixture.nats(), &instance, CLAIM_WAIT), async {
        tokio::time::sleep(Duration::from_secs(1)).await;
        instance.complete_run(&first).await;
    });
    let second =
        served.expect_run("the next claim is served with the waiting Job once the live Run ends");
    assert_ne!(runner::job_id(&second), runner::job_id(&first));
    instance.start_run(&second).await;
    let executing = br_test_harness::wait_until(LONG, || async {
        dispatch::runner_type_view(&client, admin, &runner_type).await["runnerType"]
            ["executingJobCount"]
            == json!(1)
    })
    .await;
    assert!(
        executing,
        "one live Run at a time: never more than the declared capacity"
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn push_capacity_follows_the_ready_instances_of_the_fleet() {
    let fixture = JobsFixture::start_with(Knobs {
        presence_ttl: infra::EXPIRING_PRESENCE_TTL,
        ..Knobs::default()
    })
    .await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let producer = Producer::new(fixture.fabric(), "projects");
    let runner_type = wire::unique_runner_type("push-fleet");
    let mut first =
        FakeRunner::new(fixture.nats(), &runner_type, "instance-a").declaring_capacity(2);
    first.connect().await;
    dispatch::await_live_instances(&client, admin, &runner_type, 1, LONG).await;

    for _ in 0..3 {
        producer
            .declare_v2(&JobDeclaration::new(&runner_type))
            .await;
    }
    let one = first.next_trigger(LONG).await;
    first.next_trigger(LONG).await;
    first.expect_no_trigger(QUIET).await;
    assert_eq!(
        first.trigger_count().await,
        2,
        "no third trigger is on the stream"
    );
    first.start_run(&one).await;
    first.complete_run(&one).await;
    first.next_trigger(LONG).await;
    first.expect_no_trigger(QUIET).await;
    assert_eq!(
        first.trigger_count().await,
        3,
        "the third trigger is published once"
    );

    let fourth = JobDeclaration::new(&runner_type);
    producer.declare_v2(&fourth).await;
    first.expect_no_trigger(QUIET).await;
    let mut draining =
        FakeRunner::new(fixture.nats(), &runner_type, "instance-d").declaring_capacity(2);
    draining.connect_as(wire::STATUS_DRAINING).await;
    dispatch::await_live_instances(&client, admin, &runner_type, 2, LONG).await;
    first.expect_no_trigger(QUIET).await;
    await_total_capacity(&client, admin, &runner_type, 2).await;
    assert_eq!(
        gql::status_of(&client, admin, fourth.job_id).await,
        "PENDING",
        "a draining Instance adds no capacity"
    );

    let mut second =
        FakeRunner::new(fixture.nats(), &runner_type, "instance-b").declaring_capacity(1);
    second.connect().await;
    let released = first.next_trigger(LONG).await;
    assert_eq!(
        runner::job_id(&released),
        fourth.job_id,
        "a second Instance frees a slot"
    );
    await_total_capacity(&client, admin, &runner_type, 3).await;
    let published = first.trigger_count().await;

    second.crash();
    dispatch::await_live_instances(&client, admin, &runner_type, 2, LONG).await;
    await_total_capacity(&client, admin, &runner_type, 2).await;
    first.expect_no_trigger(QUIET).await;
    assert_eq!(
        first.trigger_count().await,
        published,
        "an expiring Instance publishes nothing"
    );
    fixture.shutdown().await;
}
