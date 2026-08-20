use std::time::Duration;

use br_core_auth::Passport;
use br_test_harness::{GraphqlClient, wait_until};
use serde_json::{Value, json};
use uuid::Uuid;

use super::docs::{
    CANCEL_JOB, DELETE_JOB, DEPRECATE_RUNNER_TYPE, FLEET, JOB_BY_SOURCE, JOB_DETAIL, JOB_LOGS,
    JOBS_LIST, MANUAL_RETRY_JOB, REACTIVATE_RUNNER_TYPE, RETIRE_RUNNER_TYPE,
};

pub async fn ready(base_url: &str) -> bool {
    let (status, _) = GraphqlClient::new(base_url).get_raw("/readyz").await;
    status == reqwest::StatusCode::OK
}

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

pub async fn deprecate_runner_type(
    gql: &GraphqlClient,
    passport: &Passport,
    runner_type: &str,
) -> Value {
    runner_type_action(gql, passport, DEPRECATE_RUNNER_TYPE, runner_type).await
}

pub async fn reactivate_runner_type(
    gql: &GraphqlClient,
    passport: &Passport,
    runner_type: &str,
) -> Value {
    runner_type_action(gql, passport, REACTIVATE_RUNNER_TYPE, runner_type).await
}

pub async fn retire_runner_type(
    gql: &GraphqlClient,
    passport: &Passport,
    runner_type: &str,
) -> Value {
    runner_type_action(gql, passport, RETIRE_RUNNER_TYPE, runner_type).await
}

async fn runner_type_action(
    gql: &GraphqlClient,
    passport: &Passport,
    document: &str,
    runner_type: &str,
) -> Value {
    gql.query(
        passport,
        document,
        json!({ "input": { "runnerType": runner_type } }),
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
    log_lines(gql, passport, job_id, None).await
}

pub async fn logs_of_run(
    gql: &GraphqlClient,
    passport: &Passport,
    job_id: Uuid,
    run_id: Uuid,
) -> Vec<Value> {
    log_lines(gql, passport, job_id, Some(run_id)).await
}

async fn log_lines(
    gql: &GraphqlClient,
    passport: &Passport,
    job_id: Uuid,
    run_id: Option<Uuid>,
) -> Vec<Value> {
    edges_of(
        &log_page(
            gql,
            passport,
            job_id,
            json!({
                "runId": run_id.map(|id| id.to_string()),
                "first": 200,
            }),
        )
        .await,
    )
    .iter()
    .map(|edge| edge["node"].clone())
    .collect()
}

pub async fn log_page(
    gql: &GraphqlClient,
    passport: &Passport,
    job_id: Uuid,
    window: Value,
) -> Value {
    let mut variables = json!({ "jobId": job_id.to_string() });
    let fields = variables
        .as_object_mut()
        .expect("the log query variables are an object");
    for (key, value) in window
        .as_object()
        .expect("a log window is an object of connection arguments")
    {
        fields.insert(key.clone(), value.clone());
    }
    let response = gql.query(passport, JOB_LOGS, variables).await;
    data_of(&response, "jobsLogs")
}

pub async fn fleet_of(gql: &GraphqlClient, passport: &Passport, runner_type: &str) -> Vec<Value> {
    fleet(gql, passport, json!(runner_type)).await
}

pub async fn fleet_of_every_type(gql: &GraphqlClient, passport: &Passport) -> Vec<Value> {
    fleet(gql, passport, Value::Null).await
}

async fn fleet(gql: &GraphqlClient, passport: &Passport, runner_type: Value) -> Vec<Value> {
    let response = gql
        .query(passport, FLEET, json!({ "runnerType": runner_type }))
        .await;
    data_of(&response, "jobsFleet")
        .as_array()
        .expect("the fleet read returns a list")
        .clone()
}

pub async fn list_jobs(gql: &GraphqlClient, passport: &Passport, filter: Value) -> Vec<Value> {
    edges_of(&list_page(gql, passport, filter, 50, None).await)
        .iter()
        .map(|edge| edge["node"].clone())
        .collect()
}

pub async fn list_page(
    gql: &GraphqlClient,
    passport: &Passport,
    filter: Value,
    first: i64,
    after: Option<&str>,
) -> Value {
    let response = gql
        .query(
            passport,
            JOBS_LIST,
            json!({ "filter": filter, "first": first, "after": after }),
        )
        .await;
    data_of(&response, "jobs")
}

pub fn edges_of(connection: &Value) -> Vec<Value> {
    connection["edges"]
        .as_array()
        .unwrap_or_else(|| panic!("a connection carries an edge list: {connection}"))
        .clone()
}

pub fn listed_ids(connection: &Value) -> Vec<String> {
    edges_of(connection)
        .iter()
        .map(|edge| {
            edge["node"]["job"]["id"]
                .as_str()
                .unwrap_or_else(|| panic!("a listed row carries its job id: {edge}"))
                .to_string()
        })
        .collect()
}

pub fn assert_lists_exactly(connection: &Value, expected: &[Uuid], what: &str) {
    let mut listed = listed_ids(connection);
    listed.sort();
    let mut wanted: Vec<String> = expected.iter().map(Uuid::to_string).collect();
    wanted.sort();
    assert_eq!(
        listed, wanted,
        "{what}: the filter must answer with exactly the jobs it names, no more: {connection}"
    );
}

pub fn assert_lists_none_of(connection: &Value, excluded: &[Uuid], what: &str) {
    let listed = listed_ids(connection);
    for id in excluded {
        assert!(
            !listed.contains(&id.to_string()),
            "{what}: job {id} does not match this filter and must not be listed: {connection}"
        );
    }
}

pub fn data_of(response: &Value, field: &str) -> Value {
    let value = &response["data"][field];
    assert!(
        !value.is_null(),
        "expected data.{field} in the response, got: {response}"
    );
    value.clone()
}

pub fn mutation_error_reason(response: &Value, what: &str) -> String {
    let reason = response["errors"][0]["extensions"]["reason"]
        .as_str()
        .unwrap_or_else(|| panic!("{what}: a refused mutation carries a stable reason: {response}"))
        .to_owned();
    super::codes::assert_stable_reason(&reason, what);
    reason
}

pub fn mutation_error_params(response: &Value, what: &str) -> Value {
    let params = response["errors"][0]["extensions"]["params"].clone();
    assert!(
        params.is_object(),
        "{what}: a state refusal carries structured params: {response}"
    );
    params
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
    super::codes::assert_stable_reason(&reason, &format!("affordance '{action}'"));
    reason
}

pub fn assert_affordances_well_formed(view: &Value, what: &str) {
    let entries = view["affordances"].as_array().unwrap_or_else(|| {
        panic!("{what}: every view the administrator reads carries its backend-owned affordances: {view}")
    });
    for entry in entries {
        let action = entry["action"].as_str().unwrap_or_else(|| {
            panic!("{what}: an affordance names the action it decides: {entry}")
        });
        assert!(
            !action.is_empty(),
            "{what}: an affordance action is never empty: {entry}"
        );
        assert!(
            entry["allowed"].is_boolean(),
            "{what}: an affordance is a decision the client renders, never a hint it interprets: \
             {entry}"
        );
        if entry["allowed"] == json!(false) {
            let reason = entry["reasonCode"].as_str().unwrap_or_else(|| {
                panic!("{what}: a blocked affordance owes its reason code: {entry}")
            });
            super::codes::assert_stable_reason(reason, &format!("{what}: affordance '{action}'"));
        }
        assert!(
            entry["params"].is_null() || entry["params"].is_object(),
            "{what}: affordance params are structured data the client renders, never prose: {entry}"
        );
    }
}

pub fn listed_row(rows: &[Value], job_id: Uuid) -> Value {
    rows.iter()
        .find(|row| row["job"]["id"] == json!(job_id.to_string()))
        .cloned()
        .unwrap_or_else(|| panic!("job {job_id} must be listed here: {rows:?}"))
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
