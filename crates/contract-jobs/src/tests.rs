use super::*;

const SUBJECTS: &[(&str, &str)] = &[
    ("CMD_JOB_CANCEL_V1", CMD_JOB_CANCEL_V1),
    ("CMD_JOB_CANCEL_V2", CMD_JOB_CANCEL_V2),
    ("CMD_JOB_CREATE_V1", CMD_JOB_CREATE_V1),
    ("CMD_JOB_CREATE_V2", CMD_JOB_CREATE_V2),
    ("CMD_JOB_FAIL_V1", CMD_JOB_FAIL_V1),
    ("CMD_JOB_FAIL_V2", CMD_JOB_FAIL_V2),
    ("CMD_JOB_FINISH_V1", CMD_JOB_FINISH_V1),
    ("CMD_JOB_FINISH_V2", CMD_JOB_FINISH_V2),
    ("CMD_JOB_CANCEL_V3", CMD_JOB_CANCEL_V3),
    ("CMD_JOB_CREATE_V3", CMD_JOB_CREATE_V3),
    ("CMD_JOB_FAIL_V3", CMD_JOB_FAIL_V3),
    ("CMD_JOB_FINISH_V3", CMD_JOB_FINISH_V3),
    ("EVT_JOB_CANCEL_ACCEPTED_V1", EVT_JOB_CANCEL_ACCEPTED_V1),
    ("EVT_JOB_CANCELLED_V2", EVT_JOB_CANCELLED_V2),
    ("EVT_JOB_COMMAND_REJECTED_V1", EVT_JOB_COMMAND_REJECTED_V1),
    ("EVT_JOB_COMPLETED_V2", EVT_JOB_COMPLETED_V2),
    ("EVT_JOB_CREATION_REJECTED_V2", EVT_JOB_CREATION_REJECTED_V2),
    ("EVT_JOB_FAILED_V2", EVT_JOB_FAILED_V2),
    ("EVT_JOB_PLAN_DECLARED_V2", EVT_JOB_PLAN_DECLARED_V2),
    ("EVT_JOB_QUEUED_V2", EVT_JOB_QUEUED_V2),
    ("EVT_JOB_STARTED_V2", EVT_JOB_STARTED_V2),
    ("EVT_JOB_STEP_STARTED_V2", EVT_JOB_STEP_STARTED_V2),
    ("EVT_RUN_ITEM_ARRIVED_V1", EVT_RUN_ITEM_ARRIVED_V1),
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
    ("CMD_JOBS_CLAIM_RUNNER_TYPE", CMD_JOBS_CLAIM_RUNNER_TYPE),
    (
        "CMD_JOBS_RUNNER_REGISTER_AGENTS_RUNNER_TYPE",
        CMD_JOBS_RUNNER_REGISTER_AGENTS_RUNNER_TYPE,
    ),
    (
        "CMD_JOBS_RUNNER_MARK_READ_RUNNER_TYPE",
        CMD_JOBS_RUNNER_MARK_READ_RUNNER_TYPE,
    ),
    (
        "CMD_JOBS_RUNNER_LIST_AGENTS_RUNNER_TYPE",
        CMD_JOBS_RUNNER_LIST_AGENTS_RUNNER_TYPE,
    ),
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
    let entry = serde_json::json!({
        "runner_type": "analyst",
        "instance_key": "pod-7",
        "runner_version": "1.4.2",
        "status": "DRAINING",
        "capacity": 3,
    });
    let presence: runner::Presence = serde_json::from_value(entry).unwrap();
    assert_eq!(presence.status, runner::RunnerStatus::Draining);
    assert_eq!(presence.status.as_str(), runner::STATUS_DRAINING);
    assert_eq!(presence.capacity.get(), 3);
}

#[test]
fn a_presence_entry_carrying_an_unknown_status_is_refused_never_read_as_ready() {
    for unknown in ["IDLE", "BUSY", "ready", "", "OFFLINE"] {
        let entry = serde_json::json!({
            "runner_type": "analyst",
            "instance_key": "pod-7",
            "runner_version": "1.4.2",
            "status": unknown,
            "capacity": 1,
        });
        let refused = serde_json::from_value::<runner::Presence>(entry);
        assert!(
            refused.is_err(),
            "'{unknown}' must not enter as a presence status",
        );
    }
}

#[test]
fn a_presence_status_round_trips_as_its_contract_code() {
    for (status, code) in [
        (runner::RunnerStatus::Ready, runner::STATUS_READY),
        (runner::RunnerStatus::Draining, runner::STATUS_DRAINING),
    ] {
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
        let failed: runner::RunFailed =
            serde_json::from_value(failed_fact(json_code(code))).unwrap();
        assert_eq!(failed.report.kind, Some(kind));
        assert_eq!(kind.as_str(), code);
    }
}

#[test]
fn a_terminal_report_naming_a_kind_outside_the_vocabulary_does_not_parse() {
    for outside in [
        "scaffold",
        "transient",
        "Permanent",
        "",
        "RETRYABLE",
        "FATAL",
    ] {
        assert!(
            serde_json::from_value::<runner::RunFailed>(failed_fact(json_code(outside))).is_err(),
            "'{outside}' must not enter as a failure kind",
        );
    }
    assert!(
        serde_json::from_value::<runner::RunFailed>(failed_fact(serde_json::json!(1))).is_err()
    );
}

#[test]
fn a_terminal_report_that_declares_no_kind_is_read_as_permanent() {
    let fact = serde_json::json!({
        "run_id": uuid::Uuid::now_v7(),
        "report": { "reason_code": "provider_unavailable" },
    });
    let failed: runner::RunFailed = serde_json::from_value(fact).unwrap();
    assert_eq!(failed.report.kind, None);
    assert_eq!(
        failed.report.kind.unwrap_or_default(),
        runner::FailureKind::Permanent
    );

    let explicit_null: runner::RunFailed =
        serde_json::from_value(failed_fact(serde_json::Value::Null)).unwrap();
    assert_eq!(explicit_null.report.kind, None);
}

#[test]
fn a_report_writes_the_kind_it_declares_and_writes_nothing_when_it_declares_none() {
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
    let written = serde_json::to_value(&declared).unwrap();
    let silent = serde_json::to_value(&unclassified).unwrap();
    assert_eq!(written["kind"], json_code(runner::FAILURE_KIND_TRANSIENT));
    assert!(
        silent.get("kind").is_none(),
        "an undeclared kind must stay absent on the wire: {silent}",
    );
}

#[test]
fn a_presence_entry_without_a_usable_capacity_is_refused_never_defaulted() {
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
        assert!(
            serde_json::from_value::<runner::Presence>(entry).is_err(),
            "capacity {capacity} must not enter",
        );
    }
    let absent = serde_json::json!({
        "runner_type": "analyst",
        "instance_key": "pod-7",
        "runner_version": "1.4.2",
        "status": "READY",
    });
    assert!(serde_json::from_value::<runner::Presence>(absent).is_err());
}

#[test]
fn a_v2_creation_without_a_priority_is_the_v1_declaration() {
    let job_id = uuid::Uuid::now_v7();
    let wire = serde_json::json!({
        "job_id": job_id,
        "runner_type": "analyst",
        "producer": "projects",
    });
    let creation: command::CreateJobV2 = serde_json::from_value(wire).unwrap();
    assert_eq!(creation.priority, None);
    assert_eq!(creation.declaration().job_id, job_id);
    assert!(
        serde_json::to_value(&creation)
            .unwrap()
            .get("priority")
            .is_none()
    );
}

#[test]
fn a_v2_priority_outside_the_scale_still_parses_so_it_can_be_rejected_by_code() {
    let wire = serde_json::json!({
        "job_id": uuid::Uuid::now_v7(),
        "runner_type": "analyst",
        "producer": "projects",
        "priority": 9,
    });
    let creation: command::CreateJobV2 = serde_json::from_value(wire).unwrap();
    assert_eq!(creation.priority, Some(9));
}

#[test]
fn a_claim_names_its_instance_its_slot_and_its_wait() {
    let claim_id = uuid::Uuid::now_v7();
    let wire = serde_json::json!({
        "instance_key": "pod-7",
        "claim_id": claim_id,
        "wait_ms": 20_000,
    });
    let claim: runner::Claim = serde_json::from_value(wire).unwrap();
    assert_eq!(claim.version, runner::WIRE_VERSION);
    assert_eq!(claim.claim_id, claim_id);
    assert_eq!(claim.wait_ms, 20_000);
}

#[test]
fn a_claim_reply_carries_what_a_trigger_carries_or_nothing() {
    let trigger = runner::Trigger {
        version: runner::WIRE_VERSION,
        run_id: uuid::Uuid::now_v7(),
        job_id: uuid::Uuid::now_v7(),
        config: None,
        attempt: 1,
        triggered_by: None,
    };
    let served = serde_json::to_value(runner::ClaimReply::served(trigger.clone())).unwrap();
    let empty = serde_json::to_value(runner::ClaimReply::empty()).unwrap();
    let refused = serde_json::to_value(runner::ClaimReply::refused(
        runner::ClaimRefusal::NotClaimMode,
    ))
    .unwrap();
    assert_eq!(served["run"], serde_json::to_value(&trigger).unwrap());
    assert!(empty.get("run").is_none() && empty.get("reason").is_none());
    assert!(refused.get("run").is_none());
    assert_eq!(refused["reason"], "not_claim_mode");
}

#[test]
fn every_claim_refusal_is_written_as_its_published_code_and_nothing_else_parses() {
    for refusal in runner::ClaimRefusal::ALL {
        let written = serde_json::to_value(refusal).unwrap();
        assert_eq!(written, json_code(refusal.as_str()));
        assert_eq!(
            serde_json::from_value::<runner::ClaimRefusal>(written).unwrap(),
            refusal
        );
    }
    for outside in ["blank_value", "NOT_CLAIM_MODE", "overloaded", ""] {
        assert!(serde_json::from_value::<runner::ClaimRefusal>(json_code(outside)).is_err());
    }
}

#[test]
fn a_busy_claim_names_how_long_to_wait_before_repeating_it() {
    let reply = serde_json::to_value(runner::ClaimReply::busy(250)).unwrap();
    assert_eq!(reply["reason"], "busy");
    assert_eq!(reply["retry_after_ms"], 250);
    assert!(reply.get("run").is_none());
    let refused = serde_json::to_value(runner::ClaimReply::refused(
        runner::ClaimRefusal::ClaimSpent,
    ))
    .unwrap();
    assert!(refused.get("retry_after_ms").is_none());
}
