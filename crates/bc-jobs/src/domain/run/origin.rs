use crate::domain::wire::db_string_serde;
use crate::error::JobsError;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub enum RunOrigin {
    Initial,
    AutomaticRetry,
    ManualRetry,
}

impl RunOrigin {
    pub fn as_db_str(&self) -> &'static str {
        match self {
            Self::Initial => "INITIAL",
            Self::AutomaticRetry => "AUTOMATIC_RETRY",
            Self::ManualRetry => "MANUAL_RETRY",
        }
    }

    pub fn from_db_str(value: &str) -> Result<Self, JobsError> {
        match value {
            "INITIAL" => Ok(Self::Initial),
            "AUTOMATIC_RETRY" => Ok(Self::AutomaticRetry),
            "MANUAL_RETRY" => Ok(Self::ManualRetry),
            other => Err(JobsError::UnknownEnumValue {
                field: "run_origin",
                value: other.to_owned(),
            }),
        }
    }

    pub fn derive(is_first_attempt: bool, job_has_predecessor: bool) -> Self {
        match (is_first_attempt, job_has_predecessor) {
            (false, _) => Self::AutomaticRetry,
            (true, true) => Self::ManualRetry,
            (true, false) => Self::Initial,
        }
    }
}

db_string_serde!(RunOrigin);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_first_run_of_a_first_job_is_initial() {
        // Given: attempt one of a job nobody retried into existence
        // When/Then: the run is the initial attempt
        assert_eq!(RunOrigin::derive(true, false), RunOrigin::Initial);
    }

    #[test]
    fn the_first_run_of_a_successor_job_is_a_manual_retry() {
        // Given: attempt one of a job created by an administrator's manual retry
        // When/Then: the run is attributed to the human intervention
        assert_eq!(RunOrigin::derive(true, true), RunOrigin::ManualRetry);
    }

    #[test]
    fn any_later_attempt_is_an_automatic_retry() {
        // Given: attempt two or beyond, on either kind of job
        // When/Then: the platform's own retry policy produced it
        assert_eq!(RunOrigin::derive(false, false), RunOrigin::AutomaticRetry);
        assert_eq!(RunOrigin::derive(false, true), RunOrigin::AutomaticRetry);
    }

    #[test]
    fn an_unknown_stored_origin_fails_loud() {
        // Given: an origin value this version does not know
        // When/Then: it is refused rather than mapped to a plausible variant
        assert!(RunOrigin::from_db_str("SCHEDULED").is_err());
    }
}
