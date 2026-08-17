use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

pub const WIRE_VERSION: u8 = 1;

pub const TRIGGER_STREAM: &str = "JOBS_TRIGGER";
pub const STATUS_STREAM: &str = "JOBS_STATUS";
pub const LOG_STREAM: &str = "JOBS_LOG";
pub const TRIGGER_FILTER: &str = "jobs.trigger.>";
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailureReport {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
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
