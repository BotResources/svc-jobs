use crate::error::JobsError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
    Cancelled,
}

impl JobStatus {
    pub fn as_db_str(&self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::InProgress => "IN_PROGRESS",
            Self::Completed => "COMPLETED",
            Self::Failed => "FAILED",
            Self::Cancelled => "CANCELLED",
        }
    }

    pub fn from_db_str(value: &str) -> Result<Self, JobsError> {
        match value {
            "PENDING" => Ok(Self::Pending),
            "IN_PROGRESS" => Ok(Self::InProgress),
            "COMPLETED" => Ok(Self::Completed),
            "FAILED" => Ok(Self::Failed),
            "CANCELLED" => Ok(Self::Cancelled),
            other => Err(JobsError::UnknownEnumValue {
                field: "job_status",
                value: other.to_owned(),
            }),
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_status_round_trips_through_its_stored_form() {
        // Given: every status this version knows
        let all = [
            JobStatus::Pending,
            JobStatus::InProgress,
            JobStatus::Completed,
            JobStatus::Failed,
            JobStatus::Cancelled,
        ];
        // When/Then: the stored form maps back to the same variant
        for status in all {
            assert_eq!(JobStatus::from_db_str(status.as_db_str()).unwrap(), status);
        }
    }

    #[test]
    fn an_unknown_stored_status_fails_loud() {
        // Given: a status written by another version of the schema
        // When/Then: it is refused rather than silently defaulted
        assert_eq!(
            JobStatus::from_db_str("PAUSED"),
            Err(JobsError::UnknownEnumValue {
                field: "job_status",
                value: "PAUSED".to_owned()
            })
        );
    }

    #[test]
    fn the_three_resolution_states_are_the_terminal_ones() {
        // Given: the job lifecycle
        // When/Then: only a resolution makes a job terminal
        assert!(!JobStatus::Pending.is_terminal());
        assert!(!JobStatus::InProgress.is_terminal());
        assert!(JobStatus::Completed.is_terminal());
        assert!(JobStatus::Failed.is_terminal());
        assert!(JobStatus::Cancelled.is_terminal());
    }
}
