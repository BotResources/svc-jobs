mod support;

use std::time::Duration;

use support::db::{self, Durable};
use support::fixture::JobsFixture;
use support::producer::{JobDeclaration, Producer};
use support::runner::{self, FakeRunner};
use support::{LONG, gql, metrics, wire};

#[tokio::test]
async fn a_termination_that_beats_the_ledger_insert_by_450_ms_still_fails_the_run() {
    let fixture = JobsFixture::start().await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let runner_type = wire::unique_runner_type("gap");
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut instance = FakeRunner::new(fixture.nats(), &runner_type, "instance-a");
    let mut consumer = instance.consume_triggers().await;
    instance.connect().await;

    let owner = sqlx::PgPool::connect(&fixture.database().owner_migration_url())
        .await
        .expect("owner connects");
    let mut held = owner.begin().await.expect("lock transaction opens");
    sqlx::query("LOCK TABLE run_triggers IN SHARE ROW EXCLUSIVE MODE")
        .execute(&mut *held)
        .await
        .expect("the insert into the ledger is held back");

    let declaration = JobDeclaration::new(&runner_type);
    let job_id = declaration.job_id;
    producer.declare(&declaration).await;
    let taken = consumer.next(LONG).await;
    taken.terminate().await;
    tokio::time::sleep(Duration::from_millis(450)).await;
    held.commit().await.expect("the ledger insert is released");

    gql::wait_for_status(&client, admin, job_id, "FAILED", Duration::from_secs(8)).await;
    metrics::await_trigger_terminations(&client, "run_failed", 1.0).await;

    owner.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_job_that_changes_while_the_termination_is_settled_has_its_run_failed_all_the_same() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let runner_type = wire::unique_runner_type("racing");
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut instance = FakeRunner::new(fixture.nats(), &runner_type, "instance-a");
    let mut consumer = instance.consume_triggers().await;
    instance.connect().await;

    let declaration = JobDeclaration::new(&runner_type);
    let job_id = declaration.job_id;
    producer.declare(&declaration).await;
    let taken = consumer.next(LONG).await;
    let run = runner::run_id(&taken.payload);
    durable
        .await_count(
            db::TRIGGERS_OF_RUN,
            run,
            1,
            "the trigger sequence is stored first",
        )
        .await;

    let owner = sqlx::PgPool::connect(&fixture.database().owner_migration_url())
        .await
        .expect("owner connects");
    let mut changing = owner.begin().await.expect("the competing write opens");
    sqlx::query("UPDATE jobs SET requested_priority = 2, effective_priority = 2 WHERE id = $1")
        .bind(job_id)
        .execute(&mut *changing)
        .await
        .expect("the job changes under the service");

    taken.terminate().await;
    await_a_write_waiting_on_the_competing_one(&owner).await;
    changing.commit().await.expect("the competing write ends");

    gql::wait_for_status(&client, admin, job_id, "FAILED", LONG).await;
    metrics::await_trigger_terminations(&client, "run_failed", 1.0).await;
    durable
        .assert_count(
            db::FAILURES_OF_RUN_FOR_REASON,
            run,
            1,
            "the run failed once, as permanent, with the reason trigger_terminated",
        )
        .await;

    owner.close().await;
    durable.close().await;
    fixture.shutdown().await;
}

async fn await_a_write_waiting_on_the_competing_one(owner: &sqlx::PgPool) {
    const PROBES: u32 = 100;
    for _ in 0..PROBES {
        let waiting: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_locks lock \
             JOIN pg_stat_activity backend ON backend.pid = lock.pid \
             WHERE NOT lock.granted AND backend.datname = current_database()",
        )
        .fetch_one(owner)
        .await
        .expect("the lock-wait probe runs");
        if waiting > 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!(
        "no write of the service ever waited on the competing one: the interleaving this \
         scenario needs never happened and it would prove nothing"
    );
}
