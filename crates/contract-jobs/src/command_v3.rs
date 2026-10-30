use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::command::TriggeredBy;

pub const CONTRACT_V3_SCHEMA_VERSION: u8 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CallMode {
    Sync,
    Background,
}

impl CallMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Sync => "SYNC",
            Self::Background => "BACKGROUND",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InlineAttachment {
    pub name: String,
    pub content_type: String,
    pub content: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "form", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Payload {
    Inline {
        message: String,
        #[serde(default)]
        attachments: Vec<InlineAttachment>,
    },
    Kept,
    Reference {
        service: String,
        document_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateJobV3 {
    pub job_id: Uuid,
    pub runner_type: String,
    pub producer: String,
    #[serde(default)]
    pub config: Option<Value>,
    #[serde(default)]
    pub payload: Option<Payload>,
    #[serde(default)]
    pub passport: Option<Value>,
    #[serde(default)]
    pub triggered_by: Option<TriggeredBy>,
    #[serde(default)]
    pub parent_job_id: Option<Uuid>,
    #[serde(default)]
    pub initial_job_id: Option<Uuid>,
    #[serde(default)]
    pub mode: Option<CallMode>,
    #[serde(default)]
    pub caller_address: Option<Uuid>,
    #[serde(default)]
    pub acting_run_id: Option<Uuid>,
    #[serde(default)]
    pub acting_instance_key: Option<String>,
    #[serde(default)]
    pub source_bc: Option<String>,
    #[serde(default)]
    pub source_entity_id: Option<Uuid>,
    #[serde(default)]
    pub max_attempts: Option<i64>,
    #[serde(default)]
    pub priority: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FinishJobV3 {
    pub job_id: Uuid,
    #[serde(default)]
    pub acting_run_id: Option<Uuid>,
    #[serde(default)]
    pub acting_instance_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailJobV3 {
    pub job_id: Uuid,
    #[serde(default)]
    pub note: Option<String>,
    #[serde(default)]
    pub acting_run_id: Option<Uuid>,
    #[serde(default)]
    pub acting_instance_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelJobV3 {
    pub job_id: Uuid,
    #[serde(default)]
    pub acting_run_id: Option<Uuid>,
    #[serde(default)]
    pub acting_instance_key: Option<String>,
}

impl FinishJobV3 {
    pub fn acting(&self) -> (Option<Uuid>, Option<&str>) {
        (self.acting_run_id, self.acting_instance_key.as_deref())
    }
}

impl FailJobV3 {
    pub fn acting(&self) -> (Option<Uuid>, Option<&str>) {
        (self.acting_run_id, self.acting_instance_key.as_deref())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActingRunError {
    InstanceWithoutRun,
    RunWithoutInstance,
}

pub fn acting_run(
    run_id: Option<Uuid>,
    instance_key: Option<&str>,
) -> Result<Option<(Uuid, &str)>, ActingRunError> {
    match (run_id, instance_key) {
        (Some(run), Some(instance)) => Ok(Some((run, instance))),
        (None, None) => Ok(None),
        (None, Some(_)) => Err(ActingRunError::InstanceWithoutRun),
        (Some(_), None) => Err(ActingRunError::RunWithoutInstance),
    }
}
