mod support;

use support::fixture::JobsFixture;
use support::producer::{JobDeclaration, Producer};
use support::runner::{self, FakeRunner};
use support::{LONG, QUIET, dispatch, gql, wire};
use uuid::Uuid;

#[tokio::test]
async fn a_manual_retry_waits_for_free_capacity_like_any_other_job() {
    let fixture = JobsFixture::start().await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let producer = Producer::new(fixture.fabric(), "projects");
    let runner_type = wire::unique_runner_type("retry-capacity");
    let mut instance = FakeRunner::new(fixture.nats(), &runner_type, "instance-a");
    instance.connect().await;
    dispatch::await_live_instances(&client, admin, &runner_type, 1, LONG).await;

    let doomed = JobDeclaration::new(&runner_type).with_max_attempts(1);
    producer.declare_v2(&doomed).await;
    let doomed_trigger = instance.next_trigger(LONG).await;
    instance.start_run(&doomed_trigger).await;
    instance
        .fail_run(&doomed_trigger, "PERMANENT", "unsupported_format", None)
        .await;
    let failed = br_test_harness::wait_until(LONG, || async {
        gql::status_of(&client, admin, doomed.job_id).await == "FAILED"
    })
    .await;
    assert!(failed, "the job fails for good");

    let occupant = JobDeclaration::new(&runner_type);
    producer.declare_v2(&occupant).await;
    let occupant_trigger = instance.next_trigger(LONG).await;
    instance.start_run(&occupant_trigger).await;

    let failed_view = gql::job_view(&client, admin, doomed.job_id).await;
    let failed_resolution_id = Uuid::parse_str(
        failed_view["job"]["resolution"]["id"]
            .as_str()
            .expect("a failed job carries its resolution"),
    )
    .expect("the resolution id is a uuid");
    let successor_id = Uuid::now_v7();
    let ack = gql::manual_retry_job(
        &client,
        admin,
        Uuid::now_v7(),
        doomed.job_id,
        successor_id,
        failed_resolution_id,
    )
    .await;
    gql::expect_success(
        &ack,
        wire::FIELD_MANUAL_RETRY_JOB,
        "the retry is accepted whatever the capacity",
    );

    instance.expect_no_trigger(QUIET).await;
    assert_eq!(
        gql::status_of(&client, admin, successor_id).await,
        "PENDING",
        "a successor finds no free capacity and waits in the ledger"
    );

    instance.complete_run(&occupant_trigger).await;
    let served = instance.next_trigger(LONG).await;
    assert_eq!(runner::job_id(&served), successor_id);
    fixture.shutdown().await;
}
