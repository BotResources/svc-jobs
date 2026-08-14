mod support;

use br_test_harness::{SseSubscription, verdict};
use reqwest::StatusCode;
use serde_json::{Value, json};
use support::events::EventLog;
use support::fixture::{JobsFixture, impersonating_member, machine_caller};
use support::producer::{JobDeclaration, Producer};
use support::runner::FakeRunner;
use support::{LONG, QUIET, SHORT, docs, gql, stream, wire};
use uuid::Uuid;

const JOB_CHANGED: &str = "jobsJobChanged";

#[tokio::test]
async fn platform_administrators_alone_can_observe_and_operate_jobs() {
    let fixture = JobsFixture::start().await;
    let events = EventLog::open(fixture.fabric()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let member = fixture.member();
    let runner_type = wire::unique_runner_type("authz");
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut instance = FakeRunner::new(fixture.nats(), &runner_type, "instance-a");

    let declaration = JobDeclaration::new(&runner_type).with_source("projects", Uuid::now_v7());
    let job_id = declaration.job_id;
    let source_entity = declaration
        .source
        .as_ref()
        .map(|(_, entity)| *entity)
        .expect("this declaration carries a source reference");
    producer.declare(&declaration).await;
    events.expect_one(wire::FACT_QUEUED, job_id, LONG).await;

    instance.connect().await;
    let trigger = instance.next_trigger(LONG).await;
    instance.start_run(&trigger).await;
    gql::wait_for_status(&client, admin, job_id, "IN_PROGRESS", LONG).await;

    let mut watch = SseSubscription::open(
        fixture.url(),
        admin,
        &docs::job_changed_subscription(job_id),
    )
    .await;
    stream::snapshot(&mut watch, JOB_CHANGED, SHORT).await;

    let admin_view = gql::job_view(&client, admin, job_id).await;
    assert_eq!(admin_view["job"]["id"], json!(job_id.to_string()));
    assert!(!gql::list_jobs(&client, admin, json!({})).await.is_empty());
    assert!(!gql::fleet_of(&client, admin, &runner_type).await.is_empty());

    for (document, variables, what) in [
        (
            docs::JOB_DETAIL,
            json!({ "id": job_id.to_string() }),
            "the job detail",
        ),
        (
            docs::JOBS_LIST,
            json!({ "filter": Value::Null, "first": 50 }),
            "the job list",
        ),
        (
            docs::JOB_BY_SOURCE,
            json!({ "bc": "projects", "entityId": source_entity.to_string() }),
            "the source lookup",
        ),
        (
            docs::JOB_LOGS,
            json!({ "jobId": job_id.to_string(), "first": 50 }),
            "the log read",
        ),
        (
            docs::FLEET,
            json!({ "runnerType": runner_type.clone() }),
            "the fleet read",
        ),
    ] {
        let refused = client.query(member, document, variables).await;
        verdict::expect_code_shaped(&refused, what);
        assert_no_job_state_leaked(&refused, job_id, what);
    }

    for caller in [
        member.clone(),
        impersonating_member(admin),
        machine_caller(),
    ] {
        let refused = client
            .query(
                &caller,
                docs::JOB_DETAIL,
                json!({ "id": job_id.to_string() }),
            )
            .await;
        verdict::expect_code_shaped(&refused, "a caller who is not a platform administrator");
        assert_no_job_state_leaked(&refused, job_id, "a non-administrator read");
    }

    let cancel = gql::cancel_job(&client, member, Uuid::now_v7(), job_id).await;
    verdict::expect_code_shaped(&cancel, "an ordinary member cancelling a job");
    let retry = gql::manual_retry_job(
        &client,
        member,
        Uuid::now_v7(),
        job_id,
        Uuid::now_v7(),
        Uuid::now_v7(),
    )
    .await;
    verdict::expect_code_shaped(&retry, "an ordinary member retrying a job");
    let delete = gql::delete_job(&client, member, job_id).await;
    verdict::expect_code_shaped(&delete, "an ordinary member deleting a job");

    let (status, body) = client
        .query_unauthenticated(docs::JOB_DETAIL, json!({ "id": job_id.to_string() }))
        .await;
    assert_ne!(
        status,
        StatusCode::OK,
        "a passport-less caller must be refused at the edge, got: {body}"
    );
    assert_no_job_state_leaked(&body, job_id, "an unauthenticated read");

    let (status, body) = client
        .query_with_passport_header(
            "not-a-passport",
            docs::JOB_DETAIL,
            json!({ "id": job_id.to_string() }),
        )
        .await;
    assert_ne!(
        status,
        StatusCode::OK,
        "a malformed passport header must be refused, got: {body}"
    );
    assert_no_job_state_leaked(&body, job_id, "a forged passport header");

    let (status, body) = client
        .post_raw(
            "/graphql",
            &[
                ("X-Passport", &passport_header(member)),
                ("Accept", "text/event-stream"),
            ],
            json!({ "query": docs::job_changed_subscription(job_id) }),
        )
        .await;
    assert!(
        status != StatusCode::OK || !body["errors"].is_null(),
        "an ordinary member must not be able to follow a job over the subscription: {body}"
    );
    assert_no_job_state_leaked(&body, job_id, "a non-administrator subscription");

    events
        .expect_none(wire::FACT_CANCELLED, job_id, QUIET)
        .await;
    let untouched = gql::job(&client, admin, job_id).await;
    assert_eq!(untouched["status"], json!("IN_PROGRESS"));
    assert!(
        untouched["resolution"].is_null(),
        "a refused mutation leaves no resolution behind: {untouched}"
    );

    instance.complete_run(&trigger).await;
    producer.finish(job_id, Uuid::now_v7()).await;
    gql::wait_for_status(&client, admin, job_id, "COMPLETED", LONG).await;
    stream::await_delta(&mut watch, JOB_CHANGED, "JobsJobCompletedEvent", LONG).await;

    events.stop().await;
    fixture.shutdown().await;
}

fn passport_header(passport: &br_core_auth::Passport) -> String {
    use br_core_auth::PassportHeader;
    passport.to_header()
}

fn assert_no_job_state_leaked(response: &Value, job_id: Uuid, what: &str) {
    let rendered = response.to_string();
    assert!(
        !rendered.contains("\"IN_PROGRESS\""),
        "{what} must not leak job state to a caller who may not read it: {response}"
    );
    assert!(
        response["data"]["jobsJob"].is_null(),
        "{what} must not return job {job_id}: {response}"
    );
}
