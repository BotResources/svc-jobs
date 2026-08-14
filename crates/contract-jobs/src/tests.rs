use super::*;

const SUBJECTS: &[(&str, &str)] = &[
    ("CMD_JOB_CANCEL_V1", CMD_JOB_CANCEL_V1),
    ("CMD_JOB_CREATE_V1", CMD_JOB_CREATE_V1),
    ("CMD_JOB_FAIL_V1", CMD_JOB_FAIL_V1),
    ("CMD_JOB_FINISH_V1", CMD_JOB_FINISH_V1),
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

fn rendered_command(coords: &CommandCoords) -> String {
    format!(
        "integration.cmd.{}.{}.{}.v{}",
        coords.receiver.as_str(),
        coords.aggregate.as_str(),
        coords.verb.as_str(),
        coords.version
    )
}

fn rendered_event(coords: &EventCoords) -> String {
    format!(
        "integration.evt.{}.{}.{}.v{}",
        coords.producer.as_str(),
        coords.aggregate.as_str(),
        coords.fact.as_str(),
        coords.version
    )
}

#[test]
fn every_coordinate_renders_its_declared_subject() {
    assert_eq!(
        rendered_command(&cmd_job_cancel_v1_coords().unwrap()),
        CMD_JOB_CANCEL_V1
    );
    assert_eq!(
        rendered_command(&cmd_job_create_v1_coords().unwrap()),
        CMD_JOB_CREATE_V1
    );
    assert_eq!(
        rendered_command(&cmd_job_fail_v1_coords().unwrap()),
        CMD_JOB_FAIL_V1
    );
    assert_eq!(
        rendered_command(&cmd_job_finish_v1_coords().unwrap()),
        CMD_JOB_FINISH_V1
    );
    assert_eq!(
        rendered_event(&evt_job_cancelled_v1_coords().unwrap()),
        EVT_JOB_CANCELLED_V1
    );
    assert_eq!(
        rendered_event(&evt_job_completed_v1_coords().unwrap()),
        EVT_JOB_COMPLETED_V1
    );
    assert_eq!(
        rendered_event(&evt_job_creation_rejected_v1_coords().unwrap()),
        EVT_JOB_CREATION_REJECTED_V1
    );
    assert_eq!(
        rendered_event(&evt_job_failed_v1_coords().unwrap()),
        EVT_JOB_FAILED_V1
    );
    assert_eq!(
        rendered_event(&evt_job_plan_declared_v1_coords().unwrap()),
        EVT_JOB_PLAN_DECLARED_V1
    );
    assert_eq!(
        rendered_event(&evt_job_queued_v1_coords().unwrap()),
        EVT_JOB_QUEUED_V1
    );
    assert_eq!(
        rendered_event(&evt_job_started_v1_coords().unwrap()),
        EVT_JOB_STARTED_V1
    );
    assert_eq!(
        rendered_event(&evt_job_step_started_v1_coords().unwrap()),
        EVT_JOB_STEP_STARTED_V1
    );
}

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
