use br_core_integration::{
    Actor, Aggregate, Bc, CommandCoords, EventCoords, EventMetadata, IntegrationCommand, PastFact,
    ServiceAccountId, Verb,
};
use serde_json::Value;
use uuid::Uuid;

use super::clock;

pub const TRIGGER_STREAM: &str = "JOBS_TRIGGER";
pub const STATUS_STREAM: &str = "JOBS_STATUS";
pub const LOG_STREAM: &str = "JOBS_LOG";

pub const TRIGGER_BIND: &str = "jobs.trigger.>";
pub const STATUS_BIND: &str = "jobs.status.>";
pub const LOG_BIND: &str = "jobs.log.>";

pub const CANCEL_BUCKET: &str = "JOBS_CANCEL";
pub const PRESENCE_BUCKET: &str = "JOBS_PRESENCE";

pub const STATUS_READY: &str = "READY";
pub const STATUS_DRAINING: &str = "DRAINING";

pub const FACT_QUEUED: &str = "queued";
pub const FACT_CREATION_REJECTED: &str = "creation_rejected";
pub const FACT_STARTED: &str = "started";
pub const FACT_PLAN_DECLARED: &str = "plan_declared";
pub const FACT_STEP_STARTED: &str = "step_started";
pub const FACT_COMPLETED: &str = "completed";
pub const FACT_FAILED: &str = "failed";
pub const FACT_CANCELLED: &str = "cancelled";

pub const ALL_FACTS: [&str; 8] = [
    FACT_QUEUED,
    FACT_CREATION_REJECTED,
    FACT_STARTED,
    FACT_PLAN_DECLARED,
    FACT_STEP_STARTED,
    FACT_COMPLETED,
    FACT_FAILED,
    FACT_CANCELLED,
];

pub const VERB_CREATE: &str = "create";
pub const VERB_CANCEL: &str = "cancel";
pub const VERB_FINISH: &str = "finish";
pub const VERB_FAIL: &str = "fail";
pub const CONTRACT_V1: u8 = 1;
pub const CONTRACT_V2: u8 = 2;

pub const ACTION_CANCEL: &str = "cancel";
pub const ACTION_MANUAL_RETRY: &str = "manual_retry";
pub const ACTION_DELETE: &str = "delete";
pub const ACTION_DISPATCH: &str = "dispatch";
pub const ACTION_DEPRECATE: &str = "deprecate";
pub const ACTION_REACTIVATE: &str = "reactivate";
pub const ACTION_RETIRE: &str = "retire";

pub const EVT_QUEUED: &str = "JobsJobQueuedEvent";
pub const EVT_RUN_DISPATCHED: &str = "JobsRunDispatchedEvent";
pub const EVT_RUN_STARTED: &str = "JobsRunStartedEvent";
pub const EVT_PLAN_DECLARED: &str = "JobsRunPlanDeclaredEvent";
pub const EVT_STEP_STARTED: &str = "JobsRunStepStartedEvent";
pub const EVT_RUN_COMPLETED: &str = "JobsRunCompletedEvent";
pub const EVT_RUN_FAILED: &str = "JobsRunFailedEvent";
pub const EVT_RUN_CANCELLED: &str = "JobsRunCancelledEvent";
pub const EVT_RETRY_SCHEDULED: &str = "JobsRetryScheduledEvent";
pub const EVT_JOB_COMPLETED: &str = "JobsJobCompletedEvent";
pub const EVT_JOB_FAILED: &str = "JobsJobFailedEvent";
pub const EVT_JOB_CANCELLED: &str = "JobsJobCancelledEvent";
pub const EVT_MANUAL_RETRY_STARTED: &str = "JobsManualRetryStartedEvent";
pub const EVT_JOB_DELETED: &str = "JobsJobDeletedEvent";
pub const EVT_AFFORDANCES_CHANGED: &str = "JobsJobAffordancesChangedEvent";

pub const KIND_TYPE_REGISTERED: &str = "RUNNER_TYPE_REGISTERED";
pub const KIND_TYPE_DEPRECATED: &str = "RUNNER_TYPE_DEPRECATED";
pub const KIND_TYPE_REACTIVATED: &str = "RUNNER_TYPE_REACTIVATED";
pub const KIND_TYPE_RETIRED: &str = "RUNNER_TYPE_RETIRED";
pub const KIND_TYPE_BECAME_RETIRABLE: &str = "RUNNER_TYPE_BECAME_RETIRABLE";
pub const KIND_INSTANCE_CONNECTED: &str = "INSTANCE_CONNECTED";
pub const KIND_INSTANCE_DISCONNECTED: &str = "INSTANCE_DISCONNECTED";
pub const KIND_INSTANCE_STATUS_REPORTED: &str = "INSTANCE_STATUS_REPORTED";
pub const KIND_JOB_BEGAN_WAITING: &str = "JOB_BEGAN_WAITING";
pub const KIND_JOB_STOPPED_WAITING: &str = "JOB_STOPPED_WAITING";
pub const KIND_JOB_BEGAN_EXECUTING: &str = "JOB_BEGAN_EXECUTING";
pub const KIND_JOB_STOPPED_EXECUTING: &str = "JOB_STOPPED_EXECUTING";

pub const REASON_INSTANCE_LOST: &str = "instance_lost";
pub const REASON_RUNNER_TYPE_UNAVAILABLE: &str = "runner_type_unavailable";
pub const REASON_ID_REUSE: &str = "id_reuse";

pub const FIELD_CANCEL_JOB: &str = "jobsCancelJob";
pub const FIELD_MANUAL_RETRY_JOB: &str = "jobsManualRetryJob";
pub const FIELD_DELETE_JOB: &str = "jobsDeleteJob";
pub const FIELD_DEPRECATE_RUNNER_TYPE: &str = "jobsDeprecateRunnerType";
pub const FIELD_REACTIVATE_RUNNER_TYPE: &str = "jobsReactivateRunnerType";
pub const FIELD_RETIRE_RUNNER_TYPE: &str = "jobsRetireRunnerType";

pub const ACTION_FORCE_PRIORITY: &str = "force_priority";
pub const ACTION_SET_DISPATCH_MODE: &str = "set_dispatch_mode";
pub const ACTION_SET_MAX_RUN_DURATION: &str = "set_max_run_duration";
pub const ACTION_CLEAR_MAX_RUN_DURATION: &str = "clear_max_run_duration";

pub const EVT_PRIORITY_FORCED: &str = "JobsJobPriorityForcedEvent";
pub const EVT_PRIORITY_RECALCULATED: &str = "JobsJobPriorityRecalculatedEvent";

pub const KIND_TYPE_DISPATCH_MODE_CHANGED: &str = "RUNNER_TYPE_DISPATCH_MODE_CHANGED";
pub const KIND_TYPE_MAX_RUN_DURATION_CHANGED: &str = "RUNNER_TYPE_MAX_RUN_DURATION_CHANGED";

pub const MODE_PUSH: &str = "PUSH";
pub const MODE_CLAIM: &str = "CLAIM";

pub const DEFAULT_REQUESTED_PRIORITY: i64 = 3;
pub const SERVICE_DEFAULT_MAX_PRIORITY: i64 = 3;
pub const LOWEST_PRIORITY: i64 = 1;
pub const HIGHEST_PRIORITY: i64 = 5;

pub const REASON_CLAIM_LOST: &str = "claim_lost";
pub const REASON_TRIGGER_STRANDED: &str = "trigger_stranded";
pub const REASON_CLAIM_SPENT: &str = "claim_spent";
pub const REASON_MALFORMED_CLAIM: &str = "malformed_claim";
pub const REASON_INSTANCE_DRAINING: &str = "instance_draining";
pub const REASON_RUNNER_TYPE_RETIRED: &str = "runner_type_retired";
pub const REASON_PARENT_JOB_TERMINAL: &str = "parent_job_terminal";
pub const REASON_BUSY: &str = "busy";

/// The closed vocabulary a refused claim may answer with — nothing else.
pub const CLAIM_REFUSALS: [&str; 7] = [
    REASON_BUSY,
    REASON_INSTANCE_UNKNOWN,
    REASON_NOT_CLAIM_MODE,
    REASON_CLAIM_SPENT,
    REASON_MALFORMED_CLAIM,
    REASON_INSTANCE_DRAINING,
    REASON_RUNNER_TYPE_RETIRED,
];
pub const REASON_INSTANCE_UNKNOWN: &str = "instance_unknown";
pub const REASON_NOT_CLAIM_MODE: &str = "not_claim_mode";
pub const REASON_INVALID_PRIORITY: &str = "invalid_priority";
pub const REASON_JOB_ALREADY_TERMINAL: &str = "job_already_terminal";
pub const REASON_JOB_NOT_FOUND: &str = "job_not_found";
pub const REASON_RUNNER_TYPE_NOT_FOUND: &str = "runner_type_not_found";
pub const REASON_INVALID_DURATION: &str = "invalid_duration";

pub const FIELD_SET_USER_MAX_PRIORITY: &str = "jobsSetUserMaxPriority";
pub const FIELD_CLEAR_USER_MAX_PRIORITY: &str = "jobsClearUserMaxPriority";
pub const FIELD_FORCE_JOB_PRIORITY: &str = "jobsForceJobPriority";
pub const FIELD_SET_DISPATCH_MODE: &str = "jobsSetRunnerTypeDispatchMode";
pub const FIELD_SET_MAX_RUN_DURATION: &str = "jobsSetRunnerTypeMaxRunDuration";
pub const FIELD_MY_MAX_PRIORITY: &str = "jobsMyMaxPriority";
pub const FIELD_CLEAR_MAX_RUN_DURATION: &str = "jobsClearRunnerTypeMaxRunDuration";

pub const CLAIM_PREFIX: &str = "jobs.claim.";

pub fn claim_subject(runner_type: &str) -> String {
    format!("{CLAIM_PREFIX}{runner_type}")
}

pub const RUNNER_TYPE_CATALOG_PREFIX: &str = "jobs.runner_type.";
pub const DISPATCH_MODE_PREFIX: &str = "jobs.dispatch_mode.";

pub fn dispatch_mode_key(runner_type: &str) -> String {
    format!("{DISPATCH_MODE_PREFIX}{runner_type}")
}

pub fn runner_type_catalog_key(runner_type: &str) -> String {
    format!("{RUNNER_TYPE_CATALOG_PREFIX}{runner_type}")
}

pub fn trigger_subject(runner_type: &str) -> String {
    format!("jobs.trigger.{runner_type}")
}

pub fn status_subject(runner_type: &str, fact: &str) -> String {
    format!("jobs.status.{runner_type}.{fact}")
}

pub fn log_subject(runner_type: &str) -> String {
    format!("jobs.log.{runner_type}")
}

pub fn presence_key(runner_type: &str, instance_key: &str) -> String {
    format!("{runner_type}.{instance_key}")
}

pub fn cancel_key(run_id: Uuid) -> String {
    run_id.to_string()
}

pub fn command_coords(verb: &str) -> CommandCoords {
    command_coords_at(verb, CONTRACT_V1)
}

pub fn command_coords_at(verb: &str, version: u8) -> CommandCoords {
    CommandCoords {
        receiver: Bc::new("jobs").expect("jobs is a valid bc segment"),
        aggregate: Aggregate::new("job").expect("job is a valid aggregate segment"),
        verb: Verb::new(verb).expect("the verb is a valid coordinate segment"),
        version,
    }
}

pub fn event_coords(fact: &str) -> EventCoords {
    EventCoords {
        producer: Bc::new("jobs").expect("jobs is a valid bc segment"),
        aggregate: Aggregate::new("job").expect("job is a valid aggregate segment"),
        fact: PastFact::new(fact).expect("the fact is a valid coordinate segment"),
        version: 1,
    }
}

pub fn all_event_coords() -> Vec<EventCoords> {
    ALL_FACTS.iter().map(|fact| event_coords(fact)).collect()
}

pub fn fact_of(subject: &str) -> String {
    subject
        .split('.')
        .nth(4)
        .unwrap_or_default()
        .to_ascii_lowercase()
}

pub fn command_envelope(
    command_id: Uuid,
    verb: &str,
    correlation_id: Uuid,
    issuer: Uuid,
    payload: Value,
) -> IntegrationCommand<Value> {
    command_envelope_at(
        command_id,
        verb,
        CONTRACT_V1,
        correlation_id,
        issuer,
        payload,
    )
}

pub fn command_envelope_at(
    command_id: Uuid,
    verb: &str,
    version: u8,
    correlation_id: Uuid,
    issuer: Uuid,
    payload: Value,
) -> IntegrationCommand<Value> {
    IntegrationCommand::new(
        command_id,
        format!("jobs.job.{verb}"),
        version,
        clock::now(),
        EventMetadata::new(
            Actor::Service(ServiceAccountId::from(issuer)),
            correlation_id,
        ),
        payload,
    )
}

pub fn unique_runner_type(prefix: &str) -> String {
    format!("{prefix}_{}", Uuid::now_v7().simple())
}
