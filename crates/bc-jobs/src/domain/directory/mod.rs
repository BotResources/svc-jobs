pub mod caller_address;
pub mod listing;
pub mod registration;

pub use crate::domain::ids::AgentId;
use crate::error::JobsError;

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
