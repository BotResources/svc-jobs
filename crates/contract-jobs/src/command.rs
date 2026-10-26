use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

pub const COMMAND_TYPE_CREATE: &str = "job.create";
pub const COMMAND_TYPE_CANCEL: &str = "job.cancel";
pub const COMMAND_TYPE_FINISH: &str = "job.finish";
pub const COMMAND_TYPE_FAIL: &str = "job.fail";

pub const SCHEMA_VERSION: u8 = 1;
/// The schema version of `job.create.v2`.
pub const CREATE_V2_SCHEMA_VERSION: u8 = 2;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum TriggeredBy {
    Identified { id: Uuid, display_name: String },
    Anonymous(Uuid),
}

impl TriggeredBy {
    pub fn id(&self) -> Uuid {
        match self {
            Self::Identified { id, .. } => *id,
            Self::Anonymous(id) => *id,
        }
    }

    pub fn display_name(&self) -> Option<&str> {
        match self {
            Self::Identified { display_name, .. } => Some(display_name),
            Self::Anonymous(_) => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateJob {
    pub job_id: Uuid,
    pub runner_type: String,
    pub producer: String,
    #[serde(default)]
    pub config: Option<Value>,
    #[serde(default)]
    pub parent_job_id: Option<Uuid>,
    #[serde(default)]
    pub triggered_by: Option<TriggeredBy>,
    #[serde(default)]
    pub source_bc: Option<String>,
    #[serde(default)]
    pub source_entity_id: Option<Uuid>,
    #[serde(default)]
    pub max_attempts: Option<i64>,
}

/// The payload of `integration.cmd.jobs.job.create.v2`: every field of
/// [`CreateJob`], unchanged, plus an optional requested priority.
///
/// `priority` is a level from 1 (lowest) to 5 (highest); absent means 3. It
/// is ignored for a child job, which takes its parent's effective priority. A
/// level outside 1–5 is answered by `creation_rejected` with reason
/// `invalid_priority` — the field is a plain integer on the wire so that the
/// refusal is a typed rejection, never an unreadable payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateJobV2 {
    pub job_id: Uuid,
    pub runner_type: String,
    pub producer: String,
    #[serde(default)]
    pub config: Option<Value>,
    #[serde(default)]
    pub parent_job_id: Option<Uuid>,
    #[serde(default)]
    pub triggered_by: Option<TriggeredBy>,
    #[serde(default)]
    pub source_bc: Option<String>,
    #[serde(default)]
    pub source_entity_id: Option<Uuid>,
    #[serde(default)]
    pub max_attempts: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<i64>,
}

impl CreateJobV2 {
    /// The version 1 fields, with the priority set aside.
    pub fn declaration(&self) -> CreateJob {
        CreateJob {
            job_id: self.job_id,
            runner_type: self.runner_type.clone(),
            producer: self.producer.clone(),
            config: self.config.clone(),
            parent_job_id: self.parent_job_id,
            triggered_by: self.triggered_by.clone(),
            source_bc: self.source_bc.clone(),
            source_entity_id: self.source_entity_id,
            max_attempts: self.max_attempts,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceError {
    HalfPair,
}

impl CreateJob {
    pub fn source(&self) -> Result<Option<(&str, Uuid)>, SourceError> {
        match (self.source_bc.as_deref(), self.source_entity_id) {
            (Some(bc), Some(entity_id)) => Ok(Some((bc, entity_id))),
            (None, None) => Ok(None),
            _ => Err(SourceError::HalfPair),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelJob {
    pub job_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FinishJob {
    pub job_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailJob {
    pub job_id: Uuid,
    #[serde(default)]
    pub note: Option<String>,
}
