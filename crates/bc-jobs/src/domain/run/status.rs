use crate::domain::wire::db_string_serde;
use crate::error::JobsError;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunStatus {
    Pending,
    Started,
    Completed,
    Failed,
    Cancelled,
}

impl RunStatus {
    pub fn as_db_str(&self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Started => "STARTED",
            Self::Completed => "COMPLETED",
            Self::Failed => "FAILED",
            Self::Cancelled => "CANCELLED",
        }
    }

    pub fn from_db_str(value: &str) -> Result<Self, JobsError> {
        match value {
            "PENDING" => Ok(Self::Pending),
            "STARTED" => Ok(Self::Started),
            "COMPLETED" => Ok(Self::Completed),
            "FAILED" => Ok(Self::Failed),
            "CANCELLED" => Ok(Self::Cancelled),
            other => Err(JobsError::UnknownEnumValue {
                field: "run_status",
                value: other.to_owned(),
            }),
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub enum RunTerminalKind {
    Completed,
    Failed,
    Cancelled,
}

impl RunTerminalKind {
    pub fn as_db_str(&self) -> &'static str {
        match self {
            Self::Completed => "COMPLETED",
            Self::Failed => "FAILED",
            Self::Cancelled => "CANCELLED",
        }
    }

    pub fn from_db_str(value: &str) -> Result<Self, JobsError> {
        match value {
            "COMPLETED" => Ok(Self::Completed),
            "FAILED" => Ok(Self::Failed),
            "CANCELLED" => Ok(Self::Cancelled),
            other => Err(JobsError::UnknownEnumValue {
                field: "run_terminal_kind",
                value: other.to_owned(),
            }),
        }
    }

    pub fn as_status(&self) -> RunStatus {
        match self {
            Self::Completed => RunStatus::Completed,
            Self::Failed => RunStatus::Failed,
            Self::Cancelled => RunStatus::Cancelled,
        }
    }
}

db_string_serde!(RunTerminalKind);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_stored_status_this_version_does_not_know_fails_loud() {
        // Given: a status value written by some other version of the schema
        // When: it is read back
        let result = RunStatus::from_db_str("PAUSED");
        // Then: hydration refuses rather than defaulting to a plausible variant
        assert_eq!(
            result,
            Err(JobsError::UnknownEnumValue {
                field: "run_status",
                value: "PAUSED".to_owned()
            })
        );
    }

    #[test]
    fn every_status_round_trips_through_its_stored_form() {
        // Given: each status this version knows
        let all = [
            RunStatus::Pending,
            RunStatus::Started,
            RunStatus::Completed,
            RunStatus::Failed,
            RunStatus::Cancelled,
        ];
        // When/Then: the stored form maps back to the same variant
        for status in all {
            assert_eq!(RunStatus::from_db_str(status.as_db_str()).unwrap(), status);
        }
    }

    #[test]
    fn only_the_three_terminal_states_are_terminal() {
        // Given: the run lifecycle
        // When/Then: pending and started are live, the rest never reopen
        assert!(!RunStatus::Pending.is_terminal());
        assert!(!RunStatus::Started.is_terminal());
        assert!(RunStatus::Completed.is_terminal());
        assert!(RunStatus::Failed.is_terminal());
        assert!(RunStatus::Cancelled.is_terminal());
    }
}
