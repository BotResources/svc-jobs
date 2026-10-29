use std::time::Duration;

use br_core_auth::Passport;
use br_test_harness::{GraphqlClient, SseSubscription};
use serde_json::{Value, json};
use uuid::Uuid;

use super::docs::{
    CLEAR_USER_MAX_PRIORITY, FORCE_JOB_PRIORITY, JOB_PRIORITY, JOBS_LIST_PRIORITY, MY_MAX_PRIORITY,
    SET_USER_MAX_PRIORITY, USER_MAX_PRIORITIES,
};
use super::wire::FIELD_USER_MAX_PRIORITIES;
use super::{MY_MAX_PRIORITY_CHANGED, USER_MAX_PRIORITIES_CHANGED, gql, stream};

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

const USER_MAXIMA_SNAPSHOT: &str = "JobsUserMaxPrioritiesSnapshot";
const USER_MAXIMUM_DELTA: &str = "JobsUserMaxPriorityDelta";

pub async fn user_max_priorities_response(
    client: &GraphqlClient,
    passport: &Passport,
    user_ids: &[Uuid],
) -> Value {
    let ids: Vec<String> = user_ids.iter().map(Uuid::to_string).collect();
    client
        .query(passport, USER_MAX_PRIORITIES, json!({ "userIds": ids }))
        .await
}

/// The maxima an administrator reads for `user_ids`, one entry per user.
pub async fn user_max_priorities(
    client: &GraphqlClient,
    passport: &Passport,
    user_ids: &[Uuid],
) -> Vec<Value> {
    let response = user_max_priorities_response(client, passport, user_ids).await;
    assert!(
        response["errors"].is_null(),
        "an administrator reads the maximum of any user: {response}"
    );
    gql::data_of(&response, FIELD_USER_MAX_PRIORITIES)
        .as_array()
        .unwrap_or_else(|| panic!("the maxima read is a list: {response}"))
        .clone()
}

/// The entry of `user_id` among the maxima read — exactly one.
pub fn maximum_of(entries: &[Value], user_id: Uuid) -> Value {
    let found: Vec<&Value> = entries
        .iter()
        .filter(|entry| entry["userId"] == json!(user_id.to_string()))
        .collect();
    assert_eq!(
        found.len(),
        1,
        "every user asked for is answered exactly once: {entries:?}"
    );
    found[0].clone()
}

/// A user's maximum as the administrator reads it: the level that applies, and
/// whether it is the service default rather than the user's own.
pub fn assert_maximum(entry: &Value, level: i64, is_default: bool, what: &str) {
    assert_eq!(
        (entry["maxPriority"].clone(), entry["isDefault"].clone()),
        (json!(level), json!(is_default)),
        "{what}: the read says which maximum applies and whether it is the default, so the screen \
         never has to know the service setting: {entry}"
    );
    gql::assert_affordances_well_formed(entry, what);
}

pub async fn opening_maxima(watch: &mut SseSubscription, timeout: Duration) -> Vec<Value> {
    let opening = stream::snapshot(watch, USER_MAX_PRIORITIES_CHANGED, timeout).await;
    assert_eq!(
        opening["__typename"],
        json!(USER_MAXIMA_SNAPSHOT),
        "the administrator's subscription first sends the maxima it watches: {opening}"
    );
    opening["userMaxPriorities"]
        .as_array()
        .unwrap_or_else(|| panic!("the opening maxima are a list: {opening}"))
        .clone()
}

/// The next view of `user_id` pushed to the watcher, and the cursor it came
/// under.
pub async fn await_maximum_upsert(
    watch: &mut SseSubscription,
    user_id: Uuid,
    timeout: Duration,
) -> (Value, String) {
    let message = stream::await_message(
        watch,
        USER_MAX_PRIORITIES_CHANGED,
        &format!("a new view of the maximum of {user_id}"),
        |message| message["__typename"] == json!(USER_MAXIMUM_DELTA),
        timeout,
    )
    .await;
    let entry = message["userMaxPriority"].clone();
    assert_eq!(
        entry["userId"],
        json!(user_id.to_string()),
        "the only change pushed here is the one of {user_id}: {message}"
    );
    let cursor = message["cursor"]
        .as_str()
        .filter(|cursor| !cursor.is_empty())
        .unwrap_or_else(|| panic!("every pushed view carries its cursor: {message}"))
        .to_owned();
    (entry, cursor)
}
