use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

pub const EVENT_TYPE_QUEUED: &str = "job.queued";
pub const EVENT_TYPE_CREATION_REJECTED: &str = "job.creation_rejected";
pub const EVENT_TYPE_STARTED: &str = "job.started";
pub const EVENT_TYPE_PLAN_DECLARED: &str = "job.plan_declared";
pub const EVENT_TYPE_STEP_STARTED: &str = "job.step_started";
pub const EVENT_TYPE_COMPLETED: &str = "job.completed";
pub const EVENT_TYPE_FAILED: &str = "job.failed";
pub const EVENT_TYPE_CANCELLED: &str = "job.cancelled";

pub const SCHEMA_VERSION: u8 = 1;

pub const REASON_ID_REUSE: &str = "id_reuse";
pub const REASON_DUPLICATE_ACTIVE_ENTITY: &str = "duplicate_active_entity";
pub const REASON_MALFORMED_PAYLOAD: &str = "malformed_payload";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobQueued {
    pub job_id: Uuid,
    pub runner_type: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobCreationRejected {
    pub job_id: Uuid,
    pub reason_code: String,
    pub params: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobStarted {
    pub job_id: Uuid,
    pub run_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobPlanDeclared {
    pub job_id: Uuid,
    pub run_id: Uuid,
    pub steps: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobStepStarted {
    pub job_id: Uuid,
    pub run_id: Uuid,
    pub index: u32,
    pub label: String,
    pub started_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobCompleted {
    pub job_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailureReport {
    pub kind: String,
    pub reason_code: String,
    pub params: Value,
    pub diagnostic: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobFailed {
    pub job_id: Uuid,
    pub failure_cause: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_report: Option<FailureReport>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobCancelled {
    pub job_id: Uuid,
}
