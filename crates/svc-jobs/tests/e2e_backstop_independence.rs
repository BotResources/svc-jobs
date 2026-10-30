mod support;

use std::time::Duration;

use support::claim;
use support::db::Durable;
use support::dispatch;
use support::fixture::{JobsFixture, Knobs};
use support::producer::{JobDeclaration, Producer};
use support::runner::{self, FakeRunner};
use support::{LONG, wire};

const RESERVATION_SECONDS: u64 = 3;
const MAX_CLAIM_WAIT_SECONDS: u64 = 2;
const TERMINALS_OF_RUN: &str = "SELECT count(*) FROM run_terminals WHERE run_id = $1";
const WAKES_OF_RUN: &str = "SELECT count(*) FROM restart_wakes WHERE run_id = $1";

#[tokio::test]
async fn a_restart_wake_that_cannot_be_read_does_not_stop_the_safety_backstops_of_the_same_sweep() {
    let fixture = JobsFixture::start_with(Knobs {
        claim_reservation_seconds: RESERVATION_SECONDS,
        claim_max_wait_seconds: MAX_CLAIM_WAIT_SECONDS,
        backstop_interval_seconds: 1,
        retry_base_delay_seconds: 1,
        ..Knobs::default()
    })
    .await;
    let durable = Durable::open(&fixture.app_url()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let producer = Producer::new(fixture.fabric(), "projects");

    let unreadable_type = wire::unique_runner_type("unreadable");
    let mut pushed = FakeRunner::new(fixture.nats(), &unreadable_type, "instance-push");
    pushed.connect().await;
    dispatch::await_live_instances(&client, admin, &unreadable_type, 1, LONG).await;
    let unreadable = JobDeclaration::new(&unreadable_type).with_max_attempts(1);
    producer.declare_v2(&unreadable).await;
    let unreadable_run = runner::run_id(&pushed.next_trigger(LONG).await);

    let lost_type = wire::unique_runner_type("lost-claim");
    let mut claiming = FakeRunner::new(fixture.nats(), &lost_type, "instance-claim");
    claiming.connect().await;
    dispatch::await_live_instances(&client, admin, &lost_type, 1, LONG).await;
    dispatch::switch_to_claim(&client, admin, &lost_type).await;
    let lost = JobDeclaration::new(&lost_type).with_max_attempts(2);
    producer.declare_v2(&lost).await;
    let claimed = claim::claim(
        fixture.nats(),
        &claiming,
        Duration::from_secs(MAX_CLAIM_WAIT_SECONDS),
    )
    .await
    .expect_run("the claim that will never be started");
    let lost_run = runner::run_id(&claimed);

    sqlx::query(r#"UPDATE jobs SET requester = '{"bogus": 1}'::jsonb WHERE id = $1"#)
        .bind(unreadable.job_id)
        .execute(durable.pool())
        .await
        .expect("the Job is made unreadable");
    sqlx::query(
        "INSERT INTO restart_wakes (run_id, requested_at) VALUES ($1, '2000-01-01T00:00:00Z')",
    )
    .bind(unreadable_run)
    .execute(durable.pool())
    .await
    .expect("the unreadable wake is queued first");

    durable
        .await_count(
            TERMINALS_OF_RUN,
            lost_run,
            1,
            "the reservation nobody started is reclaimed although a wake before it cannot be read",
        )
        .await;
    durable
        .assert_count(
            WAKES_OF_RUN,
            unreadable_run,
            1,
            "the wake that failed stays queued for a later sweep",
        )
        .await;
    durable.close().await;
    fixture.shutdown().await;
}
