use std::time::Duration;

use br_core_auth::Passport;
use br_test_harness::{GraphqlClient, wait_until};
use serde_json::{Value, json};
use uuid::Uuid;

use super::docs::{
    CANCEL_JOB, DELETE_JOB, FLEET, JOB_BY_SOURCE, JOB_DETAIL, JOB_LOGS, JOBS_LIST, MANUAL_RETRY_JOB,
};

pub async fn cancel_job(
    gql: &GraphqlClient,
    passport: &Passport,
    resolution_id: Uuid,
    job_id: Uuid,
) -> Value {
    gql.query(
        passport,
        CANCEL_JOB,
        json!({ "input": { "id": resolution_id.to_string(), "jobId": job_id.to_string() } }),
    )
    .await
}

pub async fn manual_retry_job(
    gql: &GraphqlClient,
    passport: &Passport,
    intervention_id: Uuid,
    job_id: Uuid,
    successor_job_id: Uuid,
    failed_resolution_id: Uuid,
) -> Value {
    gql.query(
        passport,
        MANUAL_RETRY_JOB,
        json!({ "input": {
            "id": intervention_id.to_string(),
            "jobId": job_id.to_string(),
            "successorJobId": successor_job_id.to_string(),
            "failedResolutionId": failed_resolution_id.to_string(),
        }}),
    )
    .await
}

pub async fn delete_job(gql: &GraphqlClient, passport: &Passport, job_id: Uuid) -> Value {
    gql.query(
        passport,
        DELETE_JOB,
        json!({ "input": { "jobId": job_id.to_string() } }),
    )
    .await
}

pub async fn job_by_source(
    gql: &GraphqlClient,
    passport: &Passport,
    bc: &str,
    entity_id: Uuid,
) -> Value {
    let response = gql
        .query(
            passport,
            JOB_BY_SOURCE,
            json!({ "bc": bc, "entityId": entity_id.to_string() }),
        )
        .await;
    assert!(
        response["errors"].is_null(),
        "the source lookup must answer without errors: {response}"
    );
    response["data"]["jobsJobBySource"].clone()
}

pub async fn job_view(gql: &GraphqlClient, passport: &Passport, id: Uuid) -> Value {
    let response = gql
        .query(passport, JOB_DETAIL, json!({ "id": id.to_string() }))
        .await;
    data_of(&response, "jobsJob")
}

pub async fn job_response(gql: &GraphqlClient, passport: &Passport, id: Uuid) -> Value {
    gql.query(passport, JOB_DETAIL, json!({ "id": id.to_string() }))
        .await
}

pub async fn job(gql: &GraphqlClient, passport: &Passport, id: Uuid) -> Value {
    job_view(gql, passport, id).await["job"].clone()
}

pub async fn status_of(gql: &GraphqlClient, passport: &Passport, id: Uuid) -> String {
    job(gql, passport, id).await["status"]
        .as_str()
        .expect("a job carries a non-null status")
        .to_string()
}

pub async fn wait_for_status(
    gql: &GraphqlClient,
    passport: &Passport,
    id: Uuid,
    expected: &str,
    timeout: Duration,
) {
    let reached = wait_until(timeout, || async {
        let response = job_response(gql, passport, id).await;
        response["data"]["jobsJob"]["job"]["status"] == json!(expected)
    })
    .await;
    assert!(
        reached,
        "job {id} never reached status {expected} within {timeout:?}; last read: {}",
        job_response(gql, passport, id).await
    );
}

pub async fn wait_for_attempts(
    gql: &GraphqlClient,
    passport: &Passport,
    id: Uuid,
    expected: i64,
    timeout: Duration,
) {
    let reached = wait_until(timeout, || async {
        let response = job_response(gql, passport, id).await;
        response["data"]["jobsJob"]["job"]["attemptCount"] == json!(expected)
    })
    .await;
    assert!(
        reached,
        "job {id} never reached {expected} attempts within {timeout:?}; last read: {}",
        job_response(gql, passport, id).await
    );
}

pub async fn logs_of(gql: &GraphqlClient, passport: &Passport, job_id: Uuid) -> Vec<Value> {
    let response = gql
        .query(
            passport,
            JOB_LOGS,
            json!({ "jobId": job_id.to_string(), "first": 200 }),
        )
        .await;
    data_of(&response, "jobsLogs")["edges"]
        .as_array()
        .expect("a log connection carries an edge list")
        .iter()
        .map(|edge| edge["node"].clone())
        .collect()
}

pub async fn fleet_of(gql: &GraphqlClient, passport: &Passport, runner_type: &str) -> Vec<Value> {
    let response = gql
        .query(passport, FLEET, json!({ "runnerType": runner_type }))
        .await;
    data_of(&response, "jobsFleet")
        .as_array()
        .expect("the fleet read returns a list")
        .clone()
}

pub async fn list_jobs(gql: &GraphqlClient, passport: &Passport, filter: Value) -> Vec<Value> {
    let response = gql
        .query(
            passport,
            JOBS_LIST,
            json!({ "filter": filter, "first": 50 }),
        )
        .await;
    data_of(&response, "jobs")["edges"]
        .as_array()
        .expect("a job connection carries an edge list")
        .iter()
        .map(|edge| edge["node"].clone())
        .collect()
}

pub fn data_of(response: &Value, field: &str) -> Value {
    let value = &response["data"][field];
    assert!(
        !value.is_null(),
        "expected data.{field} in the response, got: {response}"
    );
    value.clone()
}

pub fn affordance(view: &Value, action: &str) -> Value {
    let affordances = view["affordances"]
        .as_array()
        .unwrap_or_else(|| panic!("expected an affordance list on the view: {view}"));
    affordances
        .iter()
        .find(|entry| entry["action"] == json!(action))
        .cloned()
        .unwrap_or_else(|| {
            panic!("no affordance named '{action}' among {affordances:?} — the backend owns the decision")
        })
}

pub fn is_allowed(view: &Value, action: &str) -> bool {
    affordance(view, action)["allowed"] == json!(true)
}

pub fn assert_allowed(view: &Value, action: &str) {
    let entry = affordance(view, action);
    assert_eq!(
        entry["allowed"],
        json!(true),
        "affordance '{action}' must be allowed here: {entry}"
    );
}

pub fn assert_blocked(view: &Value, action: &str) -> String {
    let entry = affordance(view, action);
    assert_eq!(
        entry["allowed"],
        json!(false),
        "affordance '{action}' must be blocked here: {entry}"
    );
    let reason = entry["reasonCode"]
        .as_str()
        .unwrap_or_else(|| panic!("a blocked affordance must carry a reason code: {entry}"))
        .to_string();
    assert!(
        br_test_harness::verdict::is_code_shaped(&reason),
        "affordance '{action}' reason must be a stable code, not prose: {reason}"
    );
    reason
}

pub fn run_by_attempt(job: &Value, attempt: i64) -> Value {
    job["runs"]
        .as_array()
        .unwrap_or_else(|| panic!("a job detail carries a run list: {job}"))
        .iter()
        .find(|run| run["attemptNumber"] == json!(attempt))
        .cloned()
        .unwrap_or_else(|| panic!("no run with attempt {attempt} on: {job}"))
}

pub fn run_by_id(job: &Value, run_id: Uuid) -> Value {
    job["runs"]
        .as_array()
        .unwrap_or_else(|| panic!("a job detail carries a run list: {job}"))
        .iter()
        .find(|run| run["id"] == json!(run_id.to_string()))
        .cloned()
        .unwrap_or_else(|| panic!("no run {run_id} on: {job}"))
}

pub fn child_of(job: &Value, child_id: Uuid) -> Value {
    job["children"]
        .as_array()
        .unwrap_or_else(|| panic!("a job detail carries a child list: {job}"))
        .iter()
        .find(|child| child["job"]["id"] == json!(child_id.to_string()))
        .cloned()
        .unwrap_or_else(|| panic!("no child {child_id} on: {job}"))
}
