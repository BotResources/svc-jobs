pub mod listing;
pub mod registration;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::JobsError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "Uuid", into = "Uuid")]
pub struct AgentId(Uuid);

impl AgentId {
    pub fn new(value: Uuid) -> Result<Self, JobsError> {
        if value.is_nil() {
            Err(JobsError::BlankValue { field: "agent_id" })
        } else {
            Ok(Self(value))
        }
    }

    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl From<AgentId> for Uuid {
    fn from(value: AgentId) -> Self {
        value.0
    }
}

impl TryFrom<Uuid> for AgentId {
    type Error = JobsError;

    fn try_from(value: Uuid) -> Result<Self, JobsError> {
        Self::new(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AgentState {
    Running,
    Finished,
}

impl AgentState {
    pub fn as_db_str(self) -> &'static str {
        match self {
            Self::Running => "RUNNING",
            Self::Finished => "FINISHED",
        }
    }

    pub fn from_db_str(value: &str) -> Result<Self, JobsError> {
        match value {
            "RUNNING" => Ok(Self::Running),
            "FINISHED" => Ok(Self::Finished),
            other => Err(JobsError::UnknownEnumValue {
                field: "agent_state",
                value: other.to_owned(),
            }),
        }
    }
}
