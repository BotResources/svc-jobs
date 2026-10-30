use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::event::FailureReport;

pub const EVENT_V2_SCHEMA_VERSION: u8 = 2;
pub const EVENT_V1_NEW_SCHEMA_VERSION: u8 = 1;

pub const EVENT_TYPE_COMMAND_REJECTED: &str = "job.command_rejected";
pub const EVENT_TYPE_CANCEL_ACCEPTED: &str = "job.cancel_accepted";
pub const EVENT_TYPE_ITEM_ARRIVED: &str = "run.item_arrived";

pub const COMMAND_TYPE_REJECTED_FINISH: &str = "job.finish";
pub const COMMAND_TYPE_REJECTED_FAIL: &str = "job.fail";
pub const COMMAND_TYPE_REJECTED_CANCEL: &str = "job.cancel";

pub const REASON_NOT_REQUESTER: &str = "not_requester";
pub const REASON_RUNNER_NOT_WORKING_THE_JOB: &str = "runner_not_working_the_job";
pub const REASON_ACTING_RUN_MISSING: &str = "acting_run_missing";
pub const REASON_ACTING_RUN_NOT_LIVE: &str = "acting_run_not_live";
pub const REASON_JOB_UNKNOWN: &str = "job_unknown";
pub const REASON_JOB_TERMINAL: &str = "job_terminal";

pub const REASON_PASSPORT_REQUIRED: &str = "passport_required";
pub const REASON_PASSPORT_USER_MISMATCH: &str = "passport_user_mismatch";
pub const REASON_PASSPORT_PARENT_MISMATCH: &str = "passport_parent_mismatch";
pub const REASON_INITIAL_JOB_MISMATCH: &str = "initial_job_mismatch";
pub const REASON_PARENT_TERMINAL: &str = "parent_terminal";
pub const REASON_PARENT_JOB_UNKNOWN: &str = "parent_job_unknown";
pub const REASON_RUNNER_TYPE_RETIRED: &str = "runner_type_retired";
pub const REASON_INVALID_PRIORITY: &str = "invalid_priority";
pub const REASON_PAYLOAD_TOO_LARGE: &str = "payload_too_large";
pub const REASON_PAYLOAD_NOT_UPLOADED: &str = "payload_not_uploaded";
pub const REASON_PAYLOAD_UPLOADER_MISMATCH: &str = "payload_uploader_mismatch";
pub const REASON_MODE_REQUIRED: &str = "mode_required";
pub const REASON_CALLER_ADDRESS_NOT_HELD: &str = "caller_address_not_held";
pub const REASON_CALLER_ADDRESS_NOT_OF_RUN: &str = "caller_address_not_of_run";
pub const REASON_MAX_ATTEMPTS_ABOVE_CEILING: &str = "max_attempts_above_ceiling";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RequesterRef {
    Service { service_key: String },
    Run { run_id: Uuid },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobQueuedV2 {
    pub job_id: Uuid,
    pub parent_job_id: Uuid,
    pub initial_job_id: Uuid,
    pub runner_type: String,
    pub requester: RequesterRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restart_of_job_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_of_job_id: Option<Uuid>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobCreationRejectedV2 {
    pub job_id: Uuid,
    pub parent_job_id: Uuid,
    pub initial_job_id: Uuid,
    pub reason_code: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobStartedV2 {
    pub job_id: Uuid,
    pub parent_job_id: Uuid,
    pub initial_job_id: Uuid,
    pub run_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobPlanDeclaredV2 {
    pub job_id: Uuid,
    pub parent_job_id: Uuid,
    pub initial_job_id: Uuid,
    pub run_id: Uuid,
    pub steps: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobStepStartedV2 {
    pub job_id: Uuid,
    pub parent_job_id: Uuid,
    pub initial_job_id: Uuid,
    pub run_id: Uuid,
    pub index: u32,
    pub label: String,
    pub started_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobCompletedV2 {
    pub job_id: Uuid,
    pub parent_job_id: Uuid,
    pub initial_job_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobFailedV2 {
    pub job_id: Uuid,
    pub parent_job_id: Uuid,
    pub initial_job_id: Uuid,
    pub requester: RequesterRef,
    pub failure_cause: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_report: Option<FailureReport>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<Uuid>,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobCancelledV2 {
    pub job_id: Uuid,
    pub parent_job_id: Uuid,
    pub initial_job_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobCancelAccepted {
    pub job_id: Uuid,
    pub parent_job_id: Uuid,
    pub initial_job_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobCommandRejected {
    pub command_type: String,
    pub job_id: Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_job_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initial_job_id: Option<Uuid>,
    pub reason_code: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ItemKind {
    Mail,
    CcNotice,
    Notice,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunItemArrived {
    pub run_id: Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub address: Option<Uuid>,
    pub item_id: Uuid,
    pub item_kind: ItemKind,
}
