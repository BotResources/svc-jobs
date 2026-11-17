//! The user surface over the Jobs that run for the caller: documents and
//! readers.

use br_core_auth::{Passport, PassportHeader};
use br_test_harness::{GraphqlClient, PassportBuilder};
use reqwest::StatusCode;
use serde_json::{Value, json};
use uuid::Uuid;

pub const MY_JOBS_CHANGED: &str = "jobsMyJobsChanged";

const AFFORDANCES: &str = "affordances { action allowed reasonCode params }";

const MY_SUMMARY: &str = "job { id status runnerType failureCause isDeleted }";

pub const MY_JOBS: &str = r#"
query($statuses: [JobsJobStatus!], $first: Int, $after: String) {
  jobsMyJobs(statuses: $statuses, first: $first, after: $after) {
    edges { cursor node { job { id status runnerType failureCause isDeleted } affordances { action allowed reasonCode params } } }
    pageInfo { hasNextPage hasPreviousPage startCursor endCursor }
  }
}
"#;

pub const MY_JOB: &str = r#"
query($id: UUID!) {
  jobsMyJob(id: $id) {
    job {
      id runnerType status parentJobId initialJobId attemptCount activeRunId
      requestedPriority effectivePriority forcedPriority createdAt contentErasedAt
      resolution { id kind failureCause causedByRunId }
      runs { id attemptNumber status failureReport { kind reasonCode params } }
    }
    affordances { action allowed reasonCode params }
  }
}
"#;

pub const MY_JOB_LOGS: &str = r#"
query($jobId: UUID!, $runId: UUID, $first: Int, $after: String, $last: Int, $before: String) {
  jobsMyJobLogs(jobId: $jobId, runId: $runId, first: $first, after: $after, last: $last, before: $before) {
    edges { cursor node { id jobId runId stepIndex level message loggedAt } }
    pageInfo { hasNextPage hasPreviousPage startCursor endCursor }
  }
}
"#;

/// A Passport of an ordinary, active user with a known id.
pub fn user(id: Uuid, name: &str) -> Passport {
    PassportBuilder::new()
        .user_id(id)
        .super_admin(false)
        .claim("display_name", name)
        .build()
}

pub fn deactivated_user(id: Uuid) -> Passport {
    PassportBuilder::new()
        .user_id(id)
        .super_admin(false)
        .active(false)
        .claim("display_name", "Deactivated User")
        .build()
}

pub fn my_jobs_changed(statuses: Option<&str>) -> String {
    let statuses = statuses.map_or(String::new(), |list| format!("statuses: [{list}], "));
    format!(
        r#"subscription {{
  jobsMyJobsChanged({statuses}window: {{ first: 50 }}) {{
    __typename
    ... on JobsJobsSnapshot {{
      cursor
      jobs {{
        edges {{ node {{ {MY_SUMMARY} {AFFORDANCES} }} }}
        pageInfo {{ hasNextPage hasPreviousPage startCursor endCursor }}
      }}
    }}
    ... on JobsJobsDelta {{
      cursor
      event {{ __typename id jobId occurredAt }}
      upserted {{ {MY_SUMMARY} {AFFORDANCES} }}
      removedIds
      pageInfo {{ hasNextPage hasPreviousPage startCursor endCursor }}
    }}
  }}
}}"#
    )
}

pub async fn my_jobs(
    gql: &GraphqlClient,
    passport: &Passport,
    statuses: Option<&[&str]>,
    first: i64,
    after: Option<&str>,
) -> Value {
    gql.query(
        passport,
        MY_JOBS,
        json!({ "statuses": statuses, "first": first, "after": after }),
    )
    .await
}

pub async fn my_job(gql: &GraphqlClient, passport: &Passport, id: Uuid) -> Value {
    gql.query(passport, MY_JOB, json!({ "id": id.to_string() }))
        .await
}

pub async fn my_job_logs(
    gql: &GraphqlClient,
    passport: &Passport,
    job_id: Uuid,
    run_id: Option<Uuid>,
    first: i64,
    after: Option<&str>,
) -> Value {
    gql.query(
        passport,
        MY_JOB_LOGS,
        json!({
            "jobId": job_id.to_string(),
            "runId": run_id.map(|id| id.to_string()),
            "first": first,
            "after": after,
        }),
    )
    .await
}

/// The rows of a `jobsMyJobs` answer, in the order given.
pub fn rows(response: &Value) -> Vec<Value> {
    response["data"]["jobsMyJobs"]["edges"]
        .as_array()
        .unwrap_or_else(|| panic!("jobsMyJobs answers with a connection: {response}"))
        .iter()
        .map(|edge| edge["node"].clone())
        .collect()
}

pub fn ids(rows: &[Value]) -> Vec<String> {
    rows.iter()
        .map(|row| {
            row["job"]["id"]
                .as_str()
                .expect("a listed job carries its id")
                .to_owned()
        })
        .collect()
}

pub fn strings(ids: &[Uuid]) -> Vec<String> {
    ids.iter().map(Uuid::to_string).collect()
}

/// The reason an operation of the user surface was refused with.
pub fn refusal(response: &Value, what: &str) -> (String, String) {
    assert!(
        response["data"].is_null()
            || response["data"]
                .as_object()
                .is_some_and(|fields| fields.values().all(Value::is_null)),
        "{what}: a refused read carries no data: {response}"
    );
    let error = &response["errors"][0]["extensions"];
    (
        error["code"]
            .as_str()
            .unwrap_or_else(|| panic!("{what}: a refusal carries a code: {response}"))
            .to_owned(),
        error["reason"].as_str().unwrap_or_default().to_owned(),
    )
}

/// A subscription posted by `passport`: its status and body, for a refusal.
pub async fn raw_subscription(
    gql: &GraphqlClient,
    passport: &Passport,
    document: &str,
) -> (StatusCode, Value) {
    gql.post_raw(
        "/graphql",
        &[
            ("X-Passport", &passport.to_header()),
            ("Accept", "text/event-stream"),
        ],
        json!({ "query": document }),
    )
    .await
}
