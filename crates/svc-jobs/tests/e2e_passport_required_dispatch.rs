mod support;

use serde_json::json;
use support::claim;
use support::db::Durable;
use support::fixture::JobsFixture;
use support::producer::{JobDeclaration, Producer};
use support::runner::FakeRunner;
use support::{LONG, QUIET, dispatch, gql, wire};

const CLAIM_WAIT: std::time::Duration = std::time::Duration::from_secs(3);

#[tokio::test]
async fn a_job_with_no_passport_is_never_sent_to_a_runner_and_fails_permanently_at_dispatch() {
    let fixture = JobsFixture::start().await;
    let client = fixture.gql();
    let producer = Producer::new(fixture.fabric(), "projects");
    let durable = Durable::open(&fixture.app_url()).await;
    let runner_type = wire::unique_runner_type("no-passport");
    let declaration = JobDeclaration::new(&runner_type).with_max_attempts(3);
    producer.declare_v2(&declaration).await;
    gql::wait_for_status(
        &client,
        fixture.admin(),
        declaration.job_id,
        "PENDING",
        LONG,
    )
    .await;
    sqlx::query("UPDATE jobs SET passport = NULL WHERE id = $1")
        .bind(declaration.job_id)
        .execute(durable.pool())
        .await
        .expect("the stored Passport is removed");

    let mut instance = FakeRunner::new(fixture.nats(), &runner_type, "instance-a");
    instance.connect().await;
    dispatch::await_live_instances(&client, fixture.admin(), &runner_type, 1, LONG).await;

    gql::wait_for_status(&client, fixture.admin(), declaration.job_id, "FAILED", LONG).await;
    let job = gql::job(&client, fixture.admin(), declaration.job_id).await;
    let runs = job["runs"].as_array().expect("the job carries its runs");
    assert_eq!(runs.len(), 1, "no second attempt after a permanent failure");
    assert_eq!(runs[0]["failureReport"]["kind"], json!("PERMANENT"));
    assert_eq!(
        runs[0]["failureReport"]["reasonCode"],
        json!("passport_required")
    );
    instance.expect_no_trigger(QUIET).await;
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_claim_never_serves_a_job_with_no_passport_and_the_job_fails_permanently() {
    let fixture = JobsFixture::start().await;
    let client = fixture.gql();
    let producer = Producer::new(fixture.fabric(), "projects");
    let durable = Durable::open(&fixture.app_url()).await;
    let runner_type = wire::unique_runner_type("no-passport-claim");
    let mut instance = FakeRunner::new(fixture.nats(), &runner_type, "instance-a");
    instance.connect().await;
    dispatch::await_live_instances(&client, fixture.admin(), &runner_type, 1, LONG).await;
    dispatch::switch_to_claim(&client, fixture.admin(), &runner_type).await;
    let declaration = JobDeclaration::new(&runner_type).with_max_attempts(3);
    producer.declare_v2(&declaration).await;
    gql::wait_for_status(
        &client,
        fixture.admin(),
        declaration.job_id,
        "PENDING",
        LONG,
    )
    .await;
    sqlx::query("UPDATE jobs SET passport = NULL WHERE id = $1")
        .bind(declaration.job_id)
        .execute(durable.pool())
        .await
        .expect("the stored Passport is removed");

    let answer = claim::claim(fixture.nats(), &instance, CLAIM_WAIT).await;
    assert!(
        answer
            .reply
            .get("run")
            .is_none_or(serde_json::Value::is_null),
        "no run is served without a Passport: {}",
        answer.reply
    );
    gql::wait_for_status(&client, fixture.admin(), declaration.job_id, "FAILED", LONG).await;
    let job = gql::job(&client, fixture.admin(), declaration.job_id).await;
    let runs = job["runs"].as_array().expect("the job carries its runs");
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0]["failureReport"]["kind"], json!("PERMANENT"));
    assert_eq!(
        runs[0]["failureReport"]["reasonCode"],
        json!("passport_required")
    );
    durable.close().await;
    fixture.shutdown().await;
}
