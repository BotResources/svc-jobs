use serde_json::{Value, json};
use uuid::Uuid;

use super::gql;
use super::wire;

pub fn projection(delta: &Value, job_id: Uuid, status: &str) -> Value {
    let job = delta["job"].clone();
    assert_eq!(
        job["id"],
        json!(job_id.to_string()),
        "a job delta carries the authoritative projection of its own job, never an invalidation \
         ping the client has to refetch on: {delta}"
    );
    assert_eq!(
        job["status"],
        json!(status),
        "the pushed projection must already be the new state: {delta}"
    );
    assert!(
        job["runs"].is_array(),
        "the pushed projection carries the full detail, runs included: {delta}"
    );
    assert!(
        delta["affordances"]
            .as_array()
            .is_some_and(|list| !list.is_empty()),
        "every job delta carries the current backend-owned affordances: {delta}"
    );
    job
}

pub fn event_of(delta: &Value, expected_type: &str, job_id: Uuid) -> Value {
    let event = delta["event"].clone();
    assert_eq!(event["__typename"], json!(expected_type));
    assert_eq!(
        event["jobId"],
        json!(job_id.to_string()),
        "every event carries the caller-supplied job id: {event}"
    );
    event
}

pub fn upserted(delta: &Value, job_id: Uuid) -> Value {
    delta["upserted"]
        .as_array()
        .unwrap_or_else(|| panic!("a list delta carries an upsert list: {delta}"))
        .iter()
        .find(|view| view["job"]["id"] == json!(job_id.to_string()))
        .cloned()
        .unwrap_or_else(|| panic!("job {job_id} must be upserted in this window: {delta}"))
}

pub fn assert_upserted_summary(delta: &Value, job_id: Uuid, status: &str) -> Value {
    let view = upserted(delta, job_id);
    assert_eq!(
        view["job"]["status"],
        json!(status),
        "the upserted summary is the new state itself: {delta}"
    );
    assert_page_info(delta);
    view
}

pub fn assert_removes(delta: &Value, job_id: Uuid) {
    let removed = delta["removedIds"]
        .as_array()
        .unwrap_or_else(|| panic!("a list delta carries a removal list: {delta}"))
        .clone();
    assert!(
        removed.contains(&json!(job_id.to_string())),
        "job {job_id} must leave this window through an exact removal, not a silent disappearance: \
         {delta}"
    );
    assert_page_info(delta);
}

pub fn assert_page_info(delta: &Value) {
    let page = &delta["pageInfo"];
    assert!(
        page["hasNextPage"].is_boolean() && page["hasPreviousPage"].is_boolean(),
        "a windowed delta carries the page info its window moved to: {delta}"
    );
}

pub fn assert_active_affordances(view: &Value) {
    gql::assert_allowed(view, wire::ACTION_CANCEL);
    gql::assert_blocked(view, wire::ACTION_MANUAL_RETRY);
    gql::assert_blocked(view, wire::ACTION_DELETE);
}

pub fn assert_failed_affordances(view: &Value) {
    gql::assert_blocked(view, wire::ACTION_CANCEL);
    gql::assert_allowed(view, wire::ACTION_MANUAL_RETRY);
    gql::assert_allowed(view, wire::ACTION_DELETE);
}

pub fn assert_completed_affordances(view: &Value) {
    gql::assert_blocked(view, wire::ACTION_CANCEL);
    gql::assert_blocked(view, wire::ACTION_MANUAL_RETRY);
    gql::assert_allowed(view, wire::ACTION_DELETE);
}

pub fn assert_cancelled_affordances(view: &Value) {
    gql::assert_blocked(view, wire::ACTION_CANCEL);
    gql::assert_blocked(view, wire::ACTION_MANUAL_RETRY);
    gql::assert_allowed(view, wire::ACTION_DELETE);
}

pub fn fleet_projection(delta: &Value, runner_type: &str) -> Value {
    let projection = delta["runnerType"].clone();
    assert_eq!(
        projection["typeKey"],
        json!(runner_type),
        "a fleet delta carries the authoritative runner-type projection: {delta}"
    );
    projection
}

pub fn assert_fleet(
    delta: &Value,
    runner_type: &str,
    waiting: i64,
    executing: i64,
    busy: i64,
    idle: i64,
) -> Value {
    let projection = fleet_projection(delta, runner_type);
    for (field, expected, what) in [
        ("waitingJobCount", waiting, "the jobs waiting on this type"),
        (
            "executingJobCount",
            executing,
            "the jobs executing on this type",
        ),
        ("busyInstanceCount", busy, "the instances carrying a run"),
        (
            "idleInstanceCount",
            idle,
            "the live instances carrying none",
        ),
    ] {
        assert_eq!(
            projection[field],
            json!(expected),
            "{what}: the fleet projection must follow the functional definition, so an \
             administrator never reads a flat fleet: {delta}"
        );
    }
    projection
}

pub fn assert_instance(
    projection: &Value,
    instance_key: &str,
    is_busy: bool,
    current_runs: &[Uuid],
    version: &str,
) -> Value {
    let instance = projection["instances"]
        .as_array()
        .unwrap_or_else(|| {
            panic!("a runner-type projection carries its live instances: {projection}")
        })
        .iter()
        .find(|entry| entry["instanceKey"] == json!(instance_key))
        .cloned()
        .unwrap_or_else(|| panic!("instance '{instance_key}' must be live in: {projection}"));
    assert_eq!(
        instance["version"],
        json!(version),
        "presence carries the version the instance announced: {instance}"
    );
    assert_eq!(
        instance["isBusy"],
        json!(is_busy),
        "the instance's own busy state is derived from the runs it carries: {instance}"
    );
    assert_eq!(
        instance["currentRunIds"],
        json!(
            current_runs
                .iter()
                .map(|run| run.to_string())
                .collect::<Vec<_>>()
        ),
        "the instance names the runs it is executing, one by one: {instance}"
    );
    instance
}

pub fn log_line(message: &Value, run_id: Uuid, step_index: Option<i64>) -> Value {
    let log = message["log"].clone();
    assert_eq!(
        log["runId"],
        json!(run_id.to_string()),
        "a log line belongs to exactly one run: {message}"
    );
    assert_eq!(
        log["stepIndex"],
        step_index.map_or(Value::Null, |index| json!(index)),
        "the step association is the runner's explicit input, never inferred from timestamps or \
         current progression: {message}"
    );
    log
}

pub fn instant(value: &Value) -> chrono::DateTime<chrono::Utc> {
    let raw = value
        .as_str()
        .unwrap_or_else(|| panic!("expected an RFC3339 timestamp, got: {value}"));
    chrono::DateTime::parse_from_rfc3339(raw)
        .unwrap_or_else(|error| panic!("a DateTime must be RFC3339: {raw} ({error})"))
        .with_timezone(&chrono::Utc)
}
