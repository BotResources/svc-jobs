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

fn failed_fact(kind: serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "run_id": uuid::Uuid::now_v7(),
        "report": {
            "kind": kind,
            "reason_code": "provider_unavailable",
            "params": {},
            "diagnostic": {},
        },
    })
}

#[test]
fn a_terminal_report_carries_one_of_exactly_two_retry_kinds() {
    // Given: the two kinds a runner may declare on a failure
    for (code, kind) in [
        (
            runner::FAILURE_KIND_TRANSIENT,
            runner::FailureKind::Transient,
        ),
        (
            runner::FAILURE_KIND_PERMANENT,
            runner::FailureKind::Permanent,
        ),
    ] {
        // When: the fact is read through the published contract
        let failed: runner::RunFailed =
            serde_json::from_value(failed_fact(json_code(code))).unwrap();
        // Then: the retry vocabulary arrives typed, not as free text to be re-parsed later
        assert_eq!(failed.report.kind, Some(kind));
        assert_eq!(kind.as_str(), code);
    }
}

#[test]
fn a_terminal_report_naming_a_kind_outside_the_vocabulary_does_not_parse() {
    // Given: reports whose kind is a word this contract does not define — the shape that
    // reached production once, a runner announcing its own name as a failure kind
    for outside in [
        "scaffold",
        "transient",
        "Permanent",
        "",
        "RETRYABLE",
        "FATAL",
    ] {
        // When/Then: the fact does not parse at all, so the producer's compiler is the only
        // place that word can be caught — never a receiver deciding what to do with it
        assert!(
            serde_json::from_value::<runner::RunFailed>(failed_fact(json_code(outside))).is_err(),
            "'{outside}' must not enter as a failure kind",
        );
    }
    // Then: a kind that is not even a string is refused on the same footing
    assert!(
        serde_json::from_value::<runner::RunFailed>(failed_fact(serde_json::json!(1))).is_err()
    );
}

#[test]
fn a_terminal_report_that_declares_no_kind_is_read_as_permanent() {
    // Given: a report from a runner that classifies nothing — legal, and unchanged by the
    // vocabulary becoming closed
    let fact = serde_json::json!({
        "run_id": uuid::Uuid::now_v7(),
        "report": { "reason_code": "provider_unavailable" },
    });
    // When: it is read through the published contract
    let failed: runner::RunFailed = serde_json::from_value(fact).unwrap();
    // Then: the absent kind is permanent — an unclassified failure never buys a retry
    assert_eq!(failed.report.kind, None);
    assert_eq!(
        failed.report.kind.unwrap_or_default(),
        runner::FailureKind::Permanent
    );

    // And: an explicit null is the same third legal form. A runner serializing its optional
    // kind as `null` writes this, and it must keep reading as "declared nothing" — not fail,
    // and not be turned into a value by a later `#[serde(default)]` on the field
    let explicit_null: runner::RunFailed =
        serde_json::from_value(failed_fact(serde_json::Value::Null)).unwrap();
    assert_eq!(explicit_null.report.kind, None);
}

#[test]
fn a_report_writes_the_kind_it_declares_and_writes_nothing_when_it_declares_none() {
    // Given: a report with a kind, and the same report without one
    let declared = runner::FailureReport {
        kind: Some(runner::FailureKind::Transient),
        reason_code: "provider_rate_limited".to_owned(),
        params: serde_json::json!({}),
        diagnostic: serde_json::json!({}),
    };
    let unclassified = runner::FailureReport {
        kind: None,
        ..declared.clone()
    };
    // When: each is written to the wire
    let written = serde_json::to_value(&declared).unwrap();
    let silent = serde_json::to_value(&unclassified).unwrap();
    // Then: the code is the wire form, and an undeclared kind writes no field at all — the
    // bytes a conforming runner sends today are the bytes it sends tomorrow
    assert_eq!(written["kind"], json_code(runner::FAILURE_KIND_TRANSIENT));
    assert!(
        silent.get("kind").is_none(),
        "an undeclared kind must stay absent on the wire: {silent}",
    );
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
