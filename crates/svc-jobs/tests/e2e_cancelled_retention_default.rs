mod support;

use br_test_harness::PassportBuilder;
use serde_json::json;
use support::db::Durable;
use support::erasure::{
    WAITING_FOR_ERASURE, assert_request_data_erased, assert_request_data_held, view_of,
};
use support::fixture::{JobsFixture, Knobs};
use support::gesture;
use support::lineage::live_runner;
use support::producer::{JobDeclaration, Producer};
use support::{LONG, gql};
use uuid::Uuid;

const NO_SWEEP_IN_THE_TEST: u64 = 3_600;

async fn started_job_with_request_data(
    fixture: &JobsFixture,
    runner_type: &mut support::runner::FakeRunner<'_>,
    max_attempts: i64,
) -> (Uuid, serde_json::Value) {
    let producer = Producer::new(fixture.fabric(), "projects");
    let uploader = PassportBuilder::new()
        .user_id(producer.account_id)
        .claim("scopes", vec!["jobs:runner"])
        .build_service();
    let declaration = JobDeclaration::new(&runner_type.runner_type)
        .with_config(json!({ "model": "m" }))
        .with_max_attempts(max_attempts);
    let stored = gesture::upload_payload(
        &fixture.gql(),
        &uploader,
        declaration.job_id,
        "the brief",
        None,
    )
    .await;
    gesture::accepted(&stored, "jobsUploadPayload");
    let mut body = declaration.payload("projects");
    body["payload"] = json!({ "form": "KEPT" });
    producer.declare_payload(body).await;
    let trigger = runner_type.next_trigger(LONG).await;
    runner_type.start_applied(fixture, &trigger).await;
    (declaration.job_id, trigger)
}

#[tokio::test]
async fn with_the_default_of_zero_days_the_cancel_itself_erases_the_request_data() {
    let fixture = JobsFixture::start_with(Knobs {
        backstop_interval_seconds: NO_SWEEP_IN_THE_TEST,
        ..Knobs::default()
    })
    .await;
    let durable = Durable::open(&fixture.app_url()).await;
    let mut x = live_runner(&fixture, "cancelled-default").await;
    let (job, _trigger) = started_job_with_request_data(&fixture, &mut x, 1).await;
    assert_request_data_held(&fixture, &durable, job, "before the cancel").await;
    durable
        .assert_count(
            support::erasure::KEPT_PAYLOADS_OF_JOB,
            job,
            1,
            "the kept payload is stored",
        )
        .await;

    gql::cancel_job(&fixture.gql(), fixture.admin(), Uuid::now_v7(), job).await;
    gql::wait_for_status(&fixture.gql(), fixture.admin(), job, "CANCELLED", LONG).await;

    assert_request_data_erased(&fixture, &durable, job, "the cancel transaction").await;
    durable
        .assert_count(
            WAITING_FOR_ERASURE,
            job,
            0,
            "nothing waits for the sweep with no delay",
        )
        .await;
    let first = view_of(&fixture, job).await["contentErasedAt"].clone();

    let uploader = PassportBuilder::new()
        .user_id(Uuid::now_v7())
        .claim("scopes", vec!["jobs:runner"])
        .build_service();
    let refused = gesture::upload_payload(&fixture.gql(), &uploader, job, "late", None).await;
    assert_eq!(
        gesture::refusal(&refused, "jobsUploadPayload"),
        "job_exists_not_yours",
        "a stranger learns nothing about the erasure"
    );
    assert_eq!(view_of(&fixture, job).await["contentErasedAt"], first);
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn the_requester_that_uploaded_the_payload_is_told_that_jobs_erased_it() {
    let fixture = JobsFixture::start_with(Knobs {
        backstop_interval_seconds: NO_SWEEP_IN_THE_TEST,
        ..Knobs::default()
    })
    .await;
    let durable = Durable::open(&fixture.app_url()).await;
    let mut x = live_runner(&fixture, "cancelled-upload").await;
    let producer = Producer::new(fixture.fabric(), "projects");
    let uploader = PassportBuilder::new()
        .user_id(producer.account_id)
        .claim("scopes", vec!["jobs:runner"])
        .build_service();
    let declaration = JobDeclaration::new(&x.runner_type).with_max_attempts(1);
    gesture::accepted(
        &gesture::upload_payload(&fixture.gql(), &uploader, declaration.job_id, "brief", None)
            .await,
        "jobsUploadPayload",
    );
    let mut body = declaration.payload("projects");
    body["payload"] = json!({ "form": "KEPT" });
    producer.declare_payload(body).await;
    let trigger = x.next_trigger(LONG).await;
    x.start_applied(&fixture, &trigger).await;

    gql::cancel_job(
        &fixture.gql(),
        fixture.admin(),
        Uuid::now_v7(),
        declaration.job_id,
    )
    .await;
    gql::wait_for_status(
        &fixture.gql(),
        fixture.admin(),
        declaration.job_id,
        "CANCELLED",
        LONG,
    )
    .await;

    let refused =
        gesture::upload_payload(&fixture.gql(), &uploader, declaration.job_id, "again", None).await;
    assert_eq!(
        gesture::refusal(&refused, "jobsUploadPayload"),
        "content_erased"
    );
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_failed_job_keeps_its_request_data_and_has_no_time_of_erasure() {
    let fixture = JobsFixture::start_with(Knobs {
        backstop_interval_seconds: NO_SWEEP_IN_THE_TEST,
        ..Knobs::default()
    })
    .await;
    let durable = Durable::open(&fixture.app_url()).await;
    let mut x = live_runner(&fixture, "failed-keeps").await;
    let (job, trigger) = started_job_with_request_data(&fixture, &mut x, 1).await;

    x.fail_run(&trigger, "PERMANENT", "provider_refused", None)
        .await;
    gql::wait_for_status(&fixture.gql(), fixture.admin(), job, "FAILED", LONG).await;

    assert_request_data_held(&fixture, &durable, job, "a failure erases no request data").await;
    durable
        .assert_count(
            support::erasure::KEPT_PAYLOADS_OF_JOB,
            job,
            1,
            "the kept payload stays for a manual retry",
        )
        .await;
    durable.close().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn the_cancel_of_a_retry_erases_nothing_of_the_failed_job_whose_kept_payload_it_shares() {
    let fixture = JobsFixture::start_with(Knobs {
        backstop_interval_seconds: NO_SWEEP_IN_THE_TEST,
        ..Knobs::default()
    })
    .await;
    let durable = Durable::open(&fixture.app_url()).await;
    let mut x = live_runner(&fixture, "shared-kept").await;
    let (origin, trigger) = started_job_with_request_data(&fixture, &mut x, 1).await;
    x.fail_run(&trigger, "PERMANENT", "provider_refused", None)
        .await;
    gql::wait_for_status(&fixture.gql(), fixture.admin(), origin, "FAILED", LONG).await;
    let resolution = Uuid::parse_str(
        gql::job_view(&fixture.gql(), fixture.admin(), origin).await["job"]["resolution"]["id"]
            .as_str()
            .expect("a failed job names its resolution"),
    )
    .unwrap();
    let successor = Uuid::now_v7();
    let retried = gql::manual_retry_job(
        &fixture.gql(),
        fixture.admin(),
        Uuid::now_v7(),
        origin,
        successor,
        resolution,
    )
    .await;
    assert!(
        retried.get("errors").is_none_or(|e| e.is_null()),
        "{retried}"
    );
    let retry = x.next_trigger(LONG).await;
    assert_eq!(support::runner::job_id(&retry), successor);

    gql::cancel_job(&fixture.gql(), fixture.admin(), Uuid::now_v7(), successor).await;
    gql::wait_for_status(
        &fixture.gql(),
        fixture.admin(),
        successor,
        "CANCELLED",
        LONG,
    )
    .await;

    assert_request_data_erased(&fixture, &durable, successor, "the cancelled retry").await;
    durable
        .assert_count(
            support::erasure::KEPT_PAYLOADS_OF_JOB,
            origin,
            1,
            "the kept payload of the failed Job is not the retry's to erase",
        )
        .await;
    assert_request_data_held(&fixture, &durable, origin, "the failed Job it retried").await;
    durable.close().await;
    fixture.shutdown().await;
}
