use br_core_integration::{
    Actor, Aggregate, Bc, CommandCoords, EventCoords, EventMetadata, IntegrationCommand, PastFact,
    ServiceAccountId, Verb,
};
use chrono::Utc;
use serde_json::Value;
use uuid::Uuid;

pub const TRIGGER_STREAM: &str = "JOBS_TRIGGER";
pub const STATUS_STREAM: &str = "JOBS_STATUS";
pub const LOG_STREAM: &str = "JOBS_LOG";

pub const TRIGGER_BIND: &str = "jobs.trigger.>";
pub const STATUS_BIND: &str = "jobs.status.>";
pub const LOG_BIND: &str = "jobs.log.>";

pub const CANCEL_BUCKET: &str = "JOBS_CANCEL";
pub const PRESENCE_BUCKET: &str = "JOBS_PRESENCE";

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

pub const ACTION_CANCEL: &str = "CANCEL_JOB";
pub const ACTION_MANUAL_RETRY: &str = "MANUAL_RETRY_JOB";
pub const ACTION_DELETE: &str = "DELETE_JOB";

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
pub const KIND_INSTANCE_CONNECTED: &str = "INSTANCE_CONNECTED";
pub const KIND_INSTANCE_DISCONNECTED: &str = "INSTANCE_DISCONNECTED";
pub const KIND_INSTANCE_STATUS_REPORTED: &str = "INSTANCE_STATUS_REPORTED";
pub const KIND_JOB_BEGAN_WAITING: &str = "JOB_BEGAN_WAITING";
pub const KIND_JOB_STOPPED_WAITING: &str = "JOB_STOPPED_WAITING";
pub const KIND_JOB_BEGAN_EXECUTING: &str = "JOB_BEGAN_EXECUTING";
pub const KIND_JOB_STOPPED_EXECUTING: &str = "JOB_STOPPED_EXECUTING";

pub const REASON_INSTANCE_LOST: &str = "instance_lost";
pub const REASON_ID_REUSE: &str = "id_reuse";

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
    CommandCoords {
        receiver: Bc::new("jobs").expect("jobs is a valid bc segment"),
        aggregate: Aggregate::new("job").expect("job is a valid aggregate segment"),
        verb: Verb::new(verb).expect("the verb is a valid coordinate segment"),
        version: 1,
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
    IntegrationCommand::new(
        command_id,
        format!("jobs.job.{verb}"),
        1,
        Utc::now(),
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
