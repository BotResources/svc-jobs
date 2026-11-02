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
        rendered_command(&cmd_job_cancel_v2_coords().unwrap()),
        CMD_JOB_CANCEL_V2
    );
    assert_eq!(
        rendered_command(&cmd_job_create_v1_coords().unwrap()),
        CMD_JOB_CREATE_V1
    );
    assert_eq!(
        rendered_command(&cmd_job_create_v2_coords().unwrap()),
        CMD_JOB_CREATE_V2
    );
    assert_eq!(
        rendered_command(&cmd_job_fail_v1_coords().unwrap()),
        CMD_JOB_FAIL_V1
    );
    assert_eq!(
        rendered_command(&cmd_job_fail_v2_coords().unwrap()),
        CMD_JOB_FAIL_V2
    );
    assert_eq!(
        rendered_command(&cmd_job_finish_v1_coords().unwrap()),
        CMD_JOB_FINISH_V1
    );
    assert_eq!(
        rendered_command(&cmd_job_finish_v2_coords().unwrap()),
        CMD_JOB_FINISH_V2
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
    assert_eq!(
        rendered_command(&cmd_job_create_v3_coords().unwrap()),
        CMD_JOB_CREATE_V3
    );
    assert_eq!(
        rendered_command(&cmd_job_cancel_v3_coords().unwrap()),
        CMD_JOB_CANCEL_V3
    );
    assert_eq!(
        rendered_command(&cmd_job_fail_v3_coords().unwrap()),
        CMD_JOB_FAIL_V3
    );
    assert_eq!(
        rendered_command(&cmd_job_finish_v3_coords().unwrap()),
        CMD_JOB_FINISH_V3
    );
    assert_eq!(
        rendered_event(&evt_job_cancel_accepted_v1_coords().unwrap()),
        EVT_JOB_CANCEL_ACCEPTED_V1
    );
    assert_eq!(
        rendered_event(&evt_job_cancelled_v2_coords().unwrap()),
        EVT_JOB_CANCELLED_V2
    );
    assert_eq!(
        rendered_event(&evt_job_command_rejected_v1_coords().unwrap()),
        EVT_JOB_COMMAND_REJECTED_V1
    );
    assert_eq!(
        rendered_event(&evt_job_completed_v2_coords().unwrap()),
        EVT_JOB_COMPLETED_V2
    );
    assert_eq!(
        rendered_event(&evt_job_creation_rejected_v2_coords().unwrap()),
        EVT_JOB_CREATION_REJECTED_V2
    );
    assert_eq!(
        rendered_event(&evt_job_failed_v2_coords().unwrap()),
        EVT_JOB_FAILED_V2
    );
    assert_eq!(
        rendered_event(&evt_job_plan_declared_v2_coords().unwrap()),
        EVT_JOB_PLAN_DECLARED_V2
    );
    assert_eq!(
        rendered_event(&evt_job_queued_v2_coords().unwrap()),
        EVT_JOB_QUEUED_V2
    );
    assert_eq!(
        rendered_event(&evt_job_started_v2_coords().unwrap()),
        EVT_JOB_STARTED_V2
    );
    assert_eq!(
        rendered_event(&evt_job_step_started_v2_coords().unwrap()),
        EVT_JOB_STEP_STARTED_V2
    );
    assert_eq!(
        rendered_event(&evt_run_item_arrived_v1_coords().unwrap()),
        EVT_RUN_ITEM_ARRIVED_V1
    );
    assert_eq!(
        rendered_event(&evt_run_item_arrived_v2_coords().unwrap()),
        EVT_RUN_ITEM_ARRIVED_V2
    );
}

#[test]
fn the_doorbell_of_version_two_is_an_event_of_the_run_aggregate() {
    let coords = evt_run_item_arrived_v2_coords().unwrap();
    assert_eq!(coords.aggregate.as_str(), "run");
    assert_eq!(coords.fact.as_str(), "item_arrived");
    assert_eq!(coords.version, 2);
    assert_eq!(coords.producer.as_str(), "jobs");
    assert_eq!(
        rendered_event(&coords),
        "integration.evt.jobs.run.item_arrived.v2"
    );
}

#[test]
fn twelve_commands_and_twenty_events_each_have_a_builder_and_only_the_doorbells_are_of_the_run() {
    let commands = [
        cmd_job_cancel_v1_coords(),
        cmd_job_cancel_v2_coords(),
        cmd_job_cancel_v3_coords(),
        cmd_job_create_v1_coords(),
        cmd_job_create_v2_coords(),
        cmd_job_create_v3_coords(),
        cmd_job_fail_v1_coords(),
        cmd_job_fail_v2_coords(),
        cmd_job_fail_v3_coords(),
        cmd_job_finish_v1_coords(),
        cmd_job_finish_v2_coords(),
        cmd_job_finish_v3_coords(),
    ];
    let events = [
        evt_job_cancel_accepted_v1_coords(),
        evt_job_cancelled_v1_coords(),
        evt_job_cancelled_v2_coords(),
        evt_job_command_rejected_v1_coords(),
        evt_job_completed_v1_coords(),
        evt_job_completed_v2_coords(),
        evt_job_creation_rejected_v1_coords(),
        evt_job_creation_rejected_v2_coords(),
        evt_job_failed_v1_coords(),
        evt_job_failed_v2_coords(),
        evt_job_plan_declared_v1_coords(),
        evt_job_plan_declared_v2_coords(),
        evt_job_queued_v1_coords(),
        evt_job_queued_v2_coords(),
        evt_job_started_v1_coords(),
        evt_job_started_v2_coords(),
        evt_job_step_started_v1_coords(),
        evt_job_step_started_v2_coords(),
        evt_run_item_arrived_v1_coords(),
        evt_run_item_arrived_v2_coords(),
    ];
    let command_subjects: Vec<String> = commands
        .iter()
        .map(|coords| rendered_command(coords.as_ref().unwrap()))
        .collect();
    let event_subjects: Vec<String> = events
        .iter()
        .map(|coords| rendered_event(coords.as_ref().unwrap()))
        .collect();
    let mut all: Vec<&String> = command_subjects.iter().chain(&event_subjects).collect();
    all.sort_unstable();
    all.dedup();
    assert_eq!(all.len(), 32);
    for subject in &event_subjects {
        let aggregate = subject.split('.').nth(3).unwrap();
        let of_the_run = subject.contains(".item_arrived.");
        assert_eq!(
            aggregate,
            if of_the_run { "run" } else { "job" },
            "{subject}"
        );
    }
}
