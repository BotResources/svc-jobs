use std::time::Duration;

use br_core_auth::Passport;
use br_test_harness::{GraphqlClient, SseSubscription};
use serde_json::{Value, json};
use uuid::Uuid;

use super::docs::{
    CLEAR_USER_MAX_PRIORITY, FORCE_JOB_PRIORITY, JOB_PRIORITY, JOBS_LIST_PRIORITY, MY_MAX_PRIORITY,
    SET_USER_MAX_PRIORITY,
};
use super::{MY_MAX_PRIORITY_CHANGED, gql, stream};

const MAX_SNAPSHOT: &str = "JobsMyMaxPrioritySnapshot";
const MAX_DELTA: &str = "JobsMyMaxPriorityDelta";

pub async fn set_user_max_priority(
    client: &GraphqlClient,
    passport: &Passport,
    user_id: Uuid,
    max_priority: i64,
) -> Value {
    client
        .query(
            passport,
            SET_USER_MAX_PRIORITY,
            json!({ "input": { "userId": user_id.to_string(), "maxPriority": max_priority } }),
        )
        .await
}

pub async fn clear_user_max_priority(
    client: &GraphqlClient,
    passport: &Passport,
    user_id: Uuid,
) -> Value {
    client
        .query(
            passport,
            CLEAR_USER_MAX_PRIORITY,
            json!({ "input": { "userId": user_id.to_string() } }),
        )
        .await
}

pub async fn force_job_priority(
    client: &GraphqlClient,
    passport: &Passport,
    job_id: Uuid,
    priority: i64,
) -> Value {
    client
        .query(
            passport,
            FORCE_JOB_PRIORITY,
            json!({ "input": { "jobId": job_id.to_string(), "priority": priority } }),
        )
        .await
}

pub async fn my_max_priority_response(client: &GraphqlClient, passport: &Passport) -> Value {
    client.query(passport, MY_MAX_PRIORITY, json!({})).await
}

pub async fn my_max_priority(client: &GraphqlClient, passport: &Passport) -> i64 {
    let response = my_max_priority_response(client, passport).await;
    assert!(
        response["errors"].is_null(),
        "any authenticated human reads their own maximum priority: {response}"
    );
    response["data"]["jobsMyMaxPriority"]["maxPriority"]
        .as_i64()
        .unwrap_or_else(|| panic!("a user's own maximum is an integer level: {response}"))
}

pub async fn job_view(client: &GraphqlClient, admin: &Passport, job_id: Uuid) -> Value {
    let response = client
        .query(admin, JOB_PRIORITY, json!({ "id": job_id.to_string() }))
        .await;
    gql::data_of(&response, "jobsJob")
}

pub async fn job(client: &GraphqlClient, admin: &Passport, job_id: Uuid) -> Value {
    job_view(client, admin, job_id).await["job"].clone()
}

pub async fn listed(client: &GraphqlClient, admin: &Passport, runner_type: &str) -> Vec<Value> {
    let response = client
        .query(
            admin,
            JOBS_LIST_PRIORITY,
            json!({ "filter": { "runnerTypes": [runner_type] }, "first": 50 }),
        )
        .await;
    gql::edges_of(&gql::data_of(&response, "jobs"))
        .iter()
        .map(|edge| edge["node"].clone())
        .collect()
}

pub fn assert_priorities(
    job: &Value,
    requested: i64,
    effective: i64,
    forced: Option<i64>,
    what: &str,
) {
    assert_eq!(
        (
            job["requestedPriority"].clone(),
            job["effectivePriority"].clone(),
            job["forcedPriority"].clone(),
        ),
        (
            json!(requested),
            json!(effective),
            forced.map_or(Value::Null, |level| json!(level)),
        ),
        "{what}: a job view carries the requested, effective and forced priority side by side, so \
         an administrator reads why a job waits where it waits without deriving it: {job}"
    );
}

pub fn assert_effective(job: &Value, effective: i64, forced: Option<i64>, what: &str) {
    assert_eq!(
        (
            job["effectivePriority"].clone(),
            job["forcedPriority"].clone()
        ),
        (
            json!(effective),
            forced.map_or(Value::Null, |level| json!(level))
        ),
        "{what}: the effective priority is the level dispatch orders this job by: {job}"
    );
}

pub async fn opening_max(watch: &mut SseSubscription, timeout: Duration) -> i64 {
    let opening = stream::snapshot(watch, MY_MAX_PRIORITY_CHANGED, timeout).await;
    assert_eq!(
        opening["__typename"],
        json!(MAX_SNAPSHOT),
        "the user's own subscription first sends their current maximum: {opening}"
    );
    opening["maxPriority"]
        .as_i64()
        .unwrap_or_else(|| panic!("the opening maximum is an integer level: {opening}"))
}

pub async fn await_max_change(
    watch: &mut SseSubscription,
    expected: i64,
    timeout: Duration,
) -> Value {
    let message = stream::await_message(
        watch,
        MY_MAX_PRIORITY_CHANGED,
        &format!("a change of the caller's own maximum to {expected}"),
        |message| message["__typename"] == json!(MAX_DELTA),
        timeout,
    )
    .await;
    assert_eq!(
        message["maxPriority"],
        json!(expected),
        "the pushed change carries the new maximum itself, never a ping to refetch: {message}"
    );
    message
}
