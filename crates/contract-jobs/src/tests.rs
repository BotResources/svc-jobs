use super::*;

const SUBJECTS: &[(&str, &str)] = &[
    ("CMD_JOB_CANCEL_V1", CMD_JOB_CANCEL_V1),
    ("CMD_JOB_CANCEL_V2", CMD_JOB_CANCEL_V2),
    ("CMD_JOB_CREATE_V1", CMD_JOB_CREATE_V1),
    ("CMD_JOB_FAIL_V1", CMD_JOB_FAIL_V1),
    ("CMD_JOB_FAIL_V2", CMD_JOB_FAIL_V2),
    ("CMD_JOB_FINISH_V1", CMD_JOB_FINISH_V1),
    ("CMD_JOB_FINISH_V2", CMD_JOB_FINISH_V2),
    ("EVT_JOB_CANCELLED_V1", EVT_JOB_CANCELLED_V1),
    ("EVT_JOB_COMPLETED_V1", EVT_JOB_COMPLETED_V1),
    ("EVT_JOB_CREATION_REJECTED_V1", EVT_JOB_CREATION_REJECTED_V1),
    ("EVT_JOB_FAILED_V1", EVT_JOB_FAILED_V1),
    ("EVT_JOB_PLAN_DECLARED_V1", EVT_JOB_PLAN_DECLARED_V1),
    ("EVT_JOB_QUEUED_V1", EVT_JOB_QUEUED_V1),
    ("EVT_JOB_STARTED_V1", EVT_JOB_STARTED_V1),
    ("EVT_JOB_STEP_STARTED_V1", EVT_JOB_STEP_STARTED_V1),
    ("EVT_JOBS_LOG_RUNNER_TYPE", EVT_JOBS_LOG_RUNNER_TYPE),
    (
        "EVT_JOBS_STATUS_RUNNER_TYPE_COMPLETED",
        EVT_JOBS_STATUS_RUNNER_TYPE_COMPLETED,
    ),
    (
        "EVT_JOBS_STATUS_RUNNER_TYPE_FAILED",
        EVT_JOBS_STATUS_RUNNER_TYPE_FAILED,
    ),
    (
        "EVT_JOBS_STATUS_RUNNER_TYPE_PLAN_DECLARED",
        EVT_JOBS_STATUS_RUNNER_TYPE_PLAN_DECLARED,
    ),
    (
        "EVT_JOBS_STATUS_RUNNER_TYPE_STARTED",
        EVT_JOBS_STATUS_RUNNER_TYPE_STARTED,
    ),
    (
        "EVT_JOBS_STATUS_RUNNER_TYPE_STEP_STARTED",
        EVT_JOBS_STATUS_RUNNER_TYPE_STEP_STARTED,
    ),
    ("CMD_JOBS_TRIGGER_RUNNER_TYPE", CMD_JOBS_TRIGGER_RUNNER_TYPE),
    ("KV_RUN_ID", KV_RUN_ID),
    ("KV_RUNNER_TYPE_INSTANCE_KEY", KV_RUNNER_TYPE_INSTANCE_KEY),
];

#[test]
fn the_service_key_is_the_registered_service_name() {
    assert_eq!(SERVICE_KEY, "jobs");
}

#[test]
fn no_two_constants_name_the_same_subject() {
    for (index, (name, subject)) in SUBJECTS.iter().enumerate() {
        assert!(!subject.is_empty(), "{name} names an empty subject");
        for (other_name, other_subject) in &SUBJECTS[index + 1..] {
            assert_ne!(
                subject, other_subject,
                "{name} and {other_name} name the same subject"
            );
        }
    }
}

#[test]
fn a_presence_entry_carries_one_of_exactly_two_status_codes() {
    // Given: a presence entry as a runner writes it
    let entry = serde_json::json!({
        "runner_type": "analyst",
        "instance_key": "pod-7",
        "runner_version": "1.4.2",
        "status": "DRAINING",
        "capacity": 3,
    });
    // When: it is read through the published contract
    let presence: runner::Presence = serde_json::from_value(entry).unwrap();
    // Then: the status is the closed code, not free text, and the capacity is a real number
    assert_eq!(presence.status, runner::RunnerStatus::Draining);
    assert_eq!(presence.status.as_str(), runner::STATUS_DRAINING);
    assert_eq!(presence.capacity.get(), 3);
}

#[test]
fn a_presence_entry_carrying_an_unknown_status_is_refused_never_read_as_ready() {
    // Given: entries whose status is not a code this contract defines
    for unknown in ["IDLE", "BUSY", "ready", "", "OFFLINE"] {
        let entry = serde_json::json!({
            "runner_type": "analyst",
            "instance_key": "pod-7",
            "runner_version": "1.4.2",
            "status": unknown,
            "capacity": 1,
        });
        // When/Then: the entry does not parse at all — it can never be mistaken for READY
        let refused = serde_json::from_value::<runner::Presence>(entry);
        assert!(
            refused.is_err(),
            "'{unknown}' must not enter as a presence status",
        );
    }
}

#[test]
fn a_presence_status_round_trips_as_its_contract_code() {
    // Given: the two statuses a runner may report
    for (status, code) in [
        (runner::RunnerStatus::Ready, runner::STATUS_READY),
        (runner::RunnerStatus::Draining, runner::STATUS_DRAINING),
    ] {
        // When/Then: the wire form is the code itself, both ways
        assert_eq!(serde_json::to_value(status).unwrap(), json_code(code));
        assert_eq!(
            serde_json::from_value::<runner::RunnerStatus>(json_code(code)).unwrap(),
            status
        );
    }
}

fn json_code(code: &str) -> serde_json::Value {
    serde_json::Value::String(code.to_owned())
}

#[test]
fn a_presence_entry_without_a_usable_capacity_is_refused_never_defaulted() {
    // Given: entries declaring no capacity, no room, or an impossible one
    for capacity in [
        serde_json::Value::Null,
        serde_json::json!(0),
        serde_json::json!(-1),
        serde_json::json!("many"),
    ] {
        let entry = serde_json::json!({
            "runner_type": "analyst",
            "instance_key": "pod-7",
            "runner_version": "1.4.2",
            "status": "READY",
            "capacity": capacity,
        });
        // When/Then: the entry does not parse — capacity is declared or the entry is ignored
        assert!(
            serde_json::from_value::<runner::Presence>(entry).is_err(),
            "capacity {capacity} must not enter",
        );
    }
    // Then: a missing field is refused too, never silently taken as one
    let absent = serde_json::json!({
        "runner_type": "analyst",
        "instance_key": "pod-7",
        "runner_version": "1.4.2",
        "status": "READY",
    });
    assert!(serde_json::from_value::<runner::Presence>(absent).is_err());
}
