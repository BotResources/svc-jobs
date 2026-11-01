use super::*;

const OFFERS: [(&str, &str); 50] = [
    ("integration.cmd.jobs.job.cancel.v1", CMD_JOB_CANCEL_V1),
    ("integration.cmd.jobs.job.cancel.v2", CMD_JOB_CANCEL_V2),
    ("integration.cmd.jobs.job.cancel.v3", CMD_JOB_CANCEL_V3),
    ("integration.cmd.jobs.job.create.v1", CMD_JOB_CREATE_V1),
    ("integration.cmd.jobs.job.create.v2", CMD_JOB_CREATE_V2),
    ("integration.cmd.jobs.job.create.v3", CMD_JOB_CREATE_V3),
    ("integration.cmd.jobs.job.fail.v1", CMD_JOB_FAIL_V1),
    ("integration.cmd.jobs.job.fail.v2", CMD_JOB_FAIL_V2),
    ("integration.cmd.jobs.job.fail.v3", CMD_JOB_FAIL_V3),
    ("integration.cmd.jobs.job.finish.v1", CMD_JOB_FINISH_V1),
    ("integration.cmd.jobs.job.finish.v2", CMD_JOB_FINISH_V2),
    ("integration.cmd.jobs.job.finish.v3", CMD_JOB_FINISH_V3),
    ("jobs.trigger.{runner_type}", CMD_JOBS_TRIGGER_RUNNER_TYPE),
    (
        "jobs.trigger.v2.{runner_type}",
        CMD_JOBS_TRIGGER_V2_RUNNER_TYPE,
    ),
    (
        "integration.evt.jobs.job.cancel_accepted.v1",
        EVT_JOB_CANCEL_ACCEPTED_V1,
    ),
    (
        "integration.evt.jobs.job.cancelled.v1",
        EVT_JOB_CANCELLED_V1,
    ),
    (
        "integration.evt.jobs.job.cancelled.v2",
        EVT_JOB_CANCELLED_V2,
    ),
    (
        "integration.evt.jobs.job.command_rejected.v1",
        EVT_JOB_COMMAND_REJECTED_V1,
    ),
    (
        "integration.evt.jobs.job.completed.v1",
        EVT_JOB_COMPLETED_V1,
    ),
    (
        "integration.evt.jobs.job.completed.v2",
        EVT_JOB_COMPLETED_V2,
    ),
    (
        "integration.evt.jobs.job.creation_rejected.v1",
        EVT_JOB_CREATION_REJECTED_V1,
    ),
    (
        "integration.evt.jobs.job.creation_rejected.v2",
        EVT_JOB_CREATION_REJECTED_V2,
    ),
    ("integration.evt.jobs.job.failed.v1", EVT_JOB_FAILED_V1),
    ("integration.evt.jobs.job.failed.v2", EVT_JOB_FAILED_V2),
    (
        "integration.evt.jobs.job.plan_declared.v1",
        EVT_JOB_PLAN_DECLARED_V1,
    ),
    (
        "integration.evt.jobs.job.plan_declared.v2",
        EVT_JOB_PLAN_DECLARED_V2,
    ),
    ("integration.evt.jobs.job.queued.v1", EVT_JOB_QUEUED_V1),
    ("integration.evt.jobs.job.queued.v2", EVT_JOB_QUEUED_V2),
    ("integration.evt.jobs.job.started.v1", EVT_JOB_STARTED_V1),
    ("integration.evt.jobs.job.started.v2", EVT_JOB_STARTED_V2),
    (
        "integration.evt.jobs.job.step_started.v1",
        EVT_JOB_STEP_STARTED_V1,
    ),
    (
        "integration.evt.jobs.job.step_started.v2",
        EVT_JOB_STEP_STARTED_V2,
    ),
    (
        "integration.evt.jobs.run.item_arrived.v1",
        EVT_RUN_ITEM_ARRIVED_V1,
    ),
    (
        "integration.evt.jobs.run.item_arrived.v2",
        EVT_RUN_ITEM_ARRIVED_V2,
    ),
    ("jobs.log.{runner_type}", EVT_JOBS_LOG_RUNNER_TYPE),
    (
        "jobs.status.{runner_type}.completed",
        EVT_JOBS_STATUS_RUNNER_TYPE_COMPLETED,
    ),
    (
        "jobs.status.{runner_type}.failed",
        EVT_JOBS_STATUS_RUNNER_TYPE_FAILED,
    ),
    (
        "jobs.status.{runner_type}.plan_declared",
        EVT_JOBS_STATUS_RUNNER_TYPE_PLAN_DECLARED,
    ),
    (
        "jobs.status.{runner_type}.started",
        EVT_JOBS_STATUS_RUNNER_TYPE_STARTED,
    ),
    (
        "jobs.status.{runner_type}.step_started",
        EVT_JOBS_STATUS_RUNNER_TYPE_STEP_STARTED,
    ),
    ("jobs.claim.{runner_type}", CMD_JOBS_CLAIM_RUNNER_TYPE),
    ("jobs.claim.v2.{runner_type}", CMD_JOBS_CLAIM_V2_RUNNER_TYPE),
    (
        "jobs.runner.register_agents.{runner_type}",
        CMD_JOBS_RUNNER_REGISTER_AGENTS_RUNNER_TYPE,
    ),
    (
        "jobs.runner.mark_read.{runner_type}",
        CMD_JOBS_RUNNER_MARK_READ_RUNNER_TYPE,
    ),
    (
        "jobs.runner.list_agents.{runner_type}",
        CMD_JOBS_RUNNER_LIST_AGENTS_RUNNER_TYPE,
    ),
    (
        "jobs.runner.started.{runner_type}",
        CMD_JOBS_RUNNER_STARTED_RUNNER_TYPE,
    ),
    ("jobs.dispatch_mode.{runner_type}", KV_JOBS_DISPATCH_MODE),
    ("jobs.runner_type.{runner_type}", KV_JOBS_RUNNER_TYPE),
    ("{run_id}", KV_RUN_ID),
    ("{runner_type}.{instance_key}", KV_RUNNER_TYPE_INSTANCE_KEY),
];

#[test]
fn the_fifty_offers_of_jobs_0_7_each_have_their_constant() {
    for (offer, constant) in OFFERS {
        assert_eq!(constant, offer);
    }
}

#[test]
fn the_fifty_offers_are_fifty_distinct_names() {
    let mut names: Vec<&str> = OFFERS.iter().map(|(offer, _)| *offer).collect();
    names.sort_unstable();
    names.dedup();
    assert_eq!(names.len(), 50);
}

#[test]
fn the_dispatch_mode_offer_has_its_constant_prefix_and_key() {
    assert_eq!(KV_JOBS_DISPATCH_MODE, "jobs.dispatch_mode.{runner_type}");
    assert_eq!(
        dispatch_mode::dispatch_mode_key("analyst"),
        "jobs.dispatch_mode.analyst"
    );
    assert_eq!(dispatch_mode::DISPATCH_MODE_PREFIX, "jobs.dispatch_mode.");
}

#[test]
fn the_stream_filters_keep_their_scopes() {
    assert_eq!(runner::TRIGGER_FILTER, "jobs.trigger.>");
    assert_eq!(runner::CLAIM_FILTER, "jobs.claim.*");
    assert_eq!(runner::CLAIM_V2_FILTER, "jobs.claim.v2.*");
    assert_eq!(runner::STATUS_FILTER, "jobs.status.>");
    assert_eq!(runner::LOG_FILTER, "jobs.log.>");
}

#[test]
fn every_reason_code_the_rejection_offers_name_has_a_text_constant() {
    use event::*;
    use event_v2::*;
    let creation = [
        (REASON_ID_REUSE, "id_reuse"),
        (REASON_DUPLICATE_ACTIVE_ENTITY, "duplicate_active_entity"),
        (REASON_MALFORMED_PAYLOAD, "malformed_payload"),
        (REASON_INVALID_PRIORITY, "invalid_priority"),
        (REASON_PASSPORT_REQUIRED, "passport_required"),
        (REASON_PASSPORT_USER_MISMATCH, "passport_user_mismatch"),
        (REASON_PASSPORT_PARENT_MISMATCH, "passport_parent_mismatch"),
        (REASON_INITIAL_JOB_MISMATCH, "initial_job_mismatch"),
        (REASON_PARENT_TERMINAL, "parent_terminal"),
        (REASON_RUNNER_TYPE_RETIRED, "runner_type_retired"),
        (REASON_PAYLOAD_TOO_LARGE, "payload_too_large"),
        (REASON_PAYLOAD_NOT_UPLOADED, "payload_not_uploaded"),
        (
            REASON_PAYLOAD_UPLOADER_MISMATCH,
            "payload_uploader_mismatch",
        ),
        (REASON_MODE_REQUIRED, "mode_required"),
        (REASON_CALLER_ADDRESS_NOT_HELD, "caller_address_not_held"),
        (
            REASON_CALLER_ADDRESS_NOT_OF_RUN,
            "caller_address_not_of_run",
        ),
        (REASON_ACTING_RUN_MISSING, "acting_run_missing"),
        (REASON_ACTING_RUN_NOT_LIVE, "acting_run_not_live"),
        (
            REASON_MAX_ATTEMPTS_ABOVE_CEILING,
            "max_attempts_above_ceiling",
        ),
        (REASON_PARENT_JOB_UNKNOWN, "parent_job_unknown"),
    ];
    let command = [
        (REASON_NOT_REQUESTER, "not_requester"),
        (
            REASON_RUNNER_NOT_WORKING_THE_JOB,
            "runner_not_working_the_job",
        ),
        (REASON_ACTING_RUN_MISSING, "acting_run_missing"),
        (REASON_ACTING_RUN_NOT_LIVE, "acting_run_not_live"),
        (REASON_JOB_UNKNOWN, "job_unknown"),
        (REASON_JOB_TERMINAL, "job_terminal"),
    ];
    for (constant, word) in creation.into_iter().chain(command) {
        assert_eq!(constant, word);
    }
    assert_eq!(
        [
            COMMAND_TYPE_REJECTED_FINISH,
            COMMAND_TYPE_REJECTED_FAIL,
            COMMAND_TYPE_REJECTED_CANCEL
        ],
        ["job.finish", "job.fail", "job.cancel"]
    );
}
