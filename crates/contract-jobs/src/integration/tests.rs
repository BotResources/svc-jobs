use super::*;
use crate::*;

use br_util_nats_fabric::{command_subject as rendered_command, event_subject as rendered_event};

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
