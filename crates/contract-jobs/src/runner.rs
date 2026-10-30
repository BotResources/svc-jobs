use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::reason::Reason;

pub const WIRE_VERSION: u8 = 1;
pub const WIRE_VERSION_V2: u8 = 2;

pub const TRIGGER_STREAM: &str = "JOBS_TRIGGER";
pub const STATUS_STREAM: &str = "JOBS_STATUS";
pub const LOG_STREAM: &str = "JOBS_LOG";
pub const TRIGGER_FILTER: &str = "jobs.trigger.>";
pub const CLAIM_FILTER: &str = "jobs.claim.*";
pub const CLAIM_V2_FILTER: &str = "jobs.claim.v2.*";
pub const STATUS_FILTER: &str = "jobs.status.>";
pub const LOG_FILTER: &str = "jobs.log.>";
pub const CANCEL_BUCKET: &str = "JOBS_CANCEL";
pub const PRESENCE_BUCKET: &str = "JOBS_PRESENCE";

fn wire_version() -> u8 {
    WIRE_VERSION
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Trigger {
    #[serde(default = "wire_version")]
    pub version: u8,
    pub run_id: Uuid,
    pub job_id: Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<Value>,
    pub attempt: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub triggered_by: Option<Uuid>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunStarted {
    #[serde(default = "wire_version")]
    pub version: u8,
    pub run_id: Uuid,
    pub instance_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanDeclared {
    #[serde(default = "wire_version")]
    pub version: u8,
    pub run_id: Uuid,
    pub steps: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub declaration_id: Option<Uuid>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepStarted {
    #[serde(default = "wire_version")]
    pub version: u8,
    pub run_id: Uuid,
    pub index: u32,
    pub label: String,
    pub started_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunCompleted {
    #[serde(default = "wire_version")]
    pub version: u8,
    pub run_id: Uuid,
}

pub const FAILURE_KIND_TRANSIENT: &str = "TRANSIENT";
pub const FAILURE_KIND_PERMANENT: &str = "PERMANENT";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum FailureKind {
    Transient,
    #[default]
    Permanent,
}

impl FailureKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Transient => FAILURE_KIND_TRANSIENT,
            Self::Permanent => FAILURE_KIND_PERMANENT,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailureReport {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<FailureKind>,
    pub reason_code: String,
    #[serde(default)]
    pub params: Value,
    #[serde(default)]
    pub diagnostic: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunFailed {
    #[serde(default = "wire_version")]
    pub version: u8,
    pub run_id: Uuid,
    pub report: FailureReport,
    #[serde(
        default,
        alias = "retry_after",
        skip_serializing_if = "Option::is_none"
    )]
    pub retry_after_seconds: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogLine {
    #[serde(default = "wire_version")]
    pub version: u8,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<Uuid>,
    pub run_id: Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step_index: Option<u32>,
    pub level: String,
    pub message: String,
    pub logged_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelRun {
    #[serde(default = "wire_version")]
    pub version: u8,
    pub run_id: Uuid,
}

pub const STATUS_READY: &str = "READY";
pub const STATUS_DRAINING: &str = "DRAINING";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RunnerStatus {
    Ready,
    Draining,
}

impl RunnerStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ready => STATUS_READY,
            Self::Draining => STATUS_DRAINING,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "u32", into = "u32")]
pub struct Capacity(u32);

impl Capacity {
    pub fn get(self) -> u32 {
        self.0
    }
}

impl TryFrom<u32> for Capacity {
    type Error = &'static str;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        if value == 0 {
            return Err("capacity declares how many runs an instance carries; it is at least 1");
        }
        Ok(Self(value))
    }
}

impl From<Capacity> for u32 {
    fn from(value: Capacity) -> Self {
        value.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Presence {
    #[serde(default = "wire_version")]
    pub version: u8,
    pub runner_type: String,
    pub instance_key: String,
    pub runner_version: String,
    pub status: RunnerStatus,
    pub capacity: Capacity,
}

pub const CLAIM_REASON_INSTANCE_UNKNOWN: &str = "instance_unknown";
pub const CLAIM_REASON_NOT_CLAIM_MODE: &str = "not_claim_mode";
pub const CLAIM_REASON_INSTANCE_DRAINING: &str = "instance_draining";
pub const CLAIM_REASON_RUNNER_TYPE_RETIRED: &str = "runner_type_retired";
pub const CLAIM_REASON_CLAIM_SPENT: &str = "claim_spent";
pub const CLAIM_REASON_MALFORMED_CLAIM: &str = "malformed_claim";
pub const CLAIM_REASON_BUSY: &str = "busy";
pub const RUN_FAILURE_CLAIM_LOST: &str = "claim_lost";
pub const RUN_CANCELLATION_TRIGGER_STRANDED: &str = "trigger_stranded";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClaimRefusal {
    InstanceUnknown,
    NotClaimMode,
    InstanceDraining,
    RunnerTypeRetired,
    ClaimSpent,
    MalformedClaim,
    Busy,
}

impl ClaimRefusal {
    pub const ALL: [Self; 7] = [
        Self::InstanceUnknown,
        Self::NotClaimMode,
        Self::InstanceDraining,
        Self::RunnerTypeRetired,
        Self::ClaimSpent,
        Self::MalformedClaim,
        Self::Busy,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::InstanceUnknown => CLAIM_REASON_INSTANCE_UNKNOWN,
            Self::NotClaimMode => CLAIM_REASON_NOT_CLAIM_MODE,
            Self::InstanceDraining => CLAIM_REASON_INSTANCE_DRAINING,
            Self::RunnerTypeRetired => CLAIM_REASON_RUNNER_TYPE_RETIRED,
            Self::ClaimSpent => CLAIM_REASON_CLAIM_SPENT,
            Self::MalformedClaim => CLAIM_REASON_MALFORMED_CLAIM,
            Self::Busy => CLAIM_REASON_BUSY,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Claim {
    #[serde(default = "wire_version")]
    pub version: u8,
    pub instance_key: String,
    pub claim_id: Uuid,
    pub wait_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimV2 {
    #[serde(default = "wire_version_v2")]
    pub version: u8,
    pub instance_key: String,
    pub claim_id: Uuid,
    pub wait_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimReply {
    #[serde(default = "wire_version")]
    pub version: u8,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run: Option<Trigger>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<ClaimRefusal>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<u64>,
}

impl ClaimReply {
    pub fn served(run: Trigger) -> Self {
        Self {
            version: WIRE_VERSION,
            run: Some(run),
            reason: None,
            retry_after_ms: None,
        }
    }

    pub fn empty() -> Self {
        Self {
            version: WIRE_VERSION,
            run: None,
            reason: None,
            retry_after_ms: None,
        }
    }

    pub fn refused(reason: ClaimRefusal) -> Self {
        Self {
            version: WIRE_VERSION,
            run: None,
            reason: Some(reason),
            retry_after_ms: None,
        }
    }

    pub fn busy(retry_after_ms: u64) -> Self {
        Self {
            version: WIRE_VERSION,
            run: None,
            reason: Some(ClaimRefusal::Busy),
            retry_after_ms: Some(retry_after_ms),
        }
    }
}

fn wire_version_v2() -> u8 {
    WIRE_VERSION_V2
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestartBlock {
    pub restart_of_run_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TriggerV2 {
    #[serde(default = "wire_version_v2")]
    pub version: u8,
    pub run_id: Uuid,
    pub job_id: Uuid,
    pub attempt: u32,
    pub parent_job_id: Uuid,
    pub initial_job_id: Uuid,
    pub passport: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub triggered_by: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<crate::command_v3::Payload>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<crate::command_v3::CallMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub caller_address: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restart: Option<RestartBlock>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimReplyV2 {
    #[serde(default = "wire_version_v2")]
    pub version: u8,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run: Option<TriggerV2>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<Reason<ClaimRefusal>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<u64>,
}

impl ClaimReplyV2 {
    pub fn served(run: TriggerV2) -> Self {
        Self {
            version: WIRE_VERSION_V2,
            run: Some(run),
            reason: None,
            retry_after_ms: None,
        }
    }

    pub fn empty() -> Self {
        Self {
            version: WIRE_VERSION_V2,
            run: None,
            reason: None,
            retry_after_ms: None,
        }
    }

    pub fn refused(reason: ClaimRefusal) -> Self {
        Self {
            version: WIRE_VERSION_V2,
            run: None,
            reason: Some(reason.into()),
            retry_after_ms: None,
        }
    }

    pub fn busy(retry_after_ms: u64) -> Self {
        Self {
            version: WIRE_VERSION_V2,
            run: None,
            reason: Some(ClaimRefusal::Busy.into()),
            retry_after_ms: Some(retry_after_ms),
        }
    }
}
