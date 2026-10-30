mod support;

use support::db::Durable;
use support::fixture::{JobsFixture, Knobs};
use support::producer::{JobDeclaration, Producer};
use support::runner::FakeRunner;
use support::{LONG, dispatch, wire};

const LIVE_ROWS: &str = "SELECT count(*) FROM current_runs WHERE job_id = $1";

#[tokio::test]
async fn the_capacity_ledger_drifted_by_an_old_pod_is_repaired_in_both_directions() {
    let fixture = JobsFixture::start_with(Knobs {
        backstop_interval_seconds: 1,
        ..Knobs::default()
    })
    .await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let durable = Durable::open(&fixture.app_url()).await;
    let producer = Producer::new(fixture.fabric(), "projects");
    let runner_type = wire::unique_runner_type("current-runs-drift");
    let mut instance = FakeRunner::new(fixture.nats(), &runner_type, "instance-a");
    instance.connect().await;
    dispatch::await_live_instances(&client, admin, &runner_type, 1, LONG).await;

    let declaration = JobDeclaration::new(&runner_type);
    producer.declare_v2(&declaration).await;
    let trigger = instance.next_trigger(LONG).await;
    instance.start_applied(&fixture, &trigger).await;
    durable
        .await_count(LIVE_ROWS, declaration.job_id, 1, "a live run holds a slot")
        .await;

    sqlx::query("DELETE FROM current_runs WHERE job_id = $1")
        .bind(declaration.job_id)
        .execute(durable.pool())
        .await
        .expect("the drift is seeded");
    durable
        .await_count(
            LIVE_ROWS,
            declaration.job_id,
            1,
            "the sweep restores the slot of a live run",
        )
        .await;

    instance.complete_run(&trigger).await;
    durable
        .await_count(
            LIVE_ROWS,
            declaration.job_id,
            0,
            "an ended run frees its slot",
        )
        .await;
    sqlx::query(
        "INSERT INTO current_runs (run_id, job_id, runner_type_id) \
         SELECT r.id, r.job_id, j.runner_type_id FROM runs r JOIN jobs j ON j.id = r.job_id \
         WHERE r.job_id = $1",
    )
    .bind(declaration.job_id)
    .execute(durable.pool())
    .await
    .expect("the stale row is seeded");
    durable
        .await_count(
            LIVE_ROWS,
            declaration.job_id,
            0,
            "the sweep drops the slot of an ended run",
        )
        .await;
    durable.close().await;
    fixture.shutdown().await;
}
