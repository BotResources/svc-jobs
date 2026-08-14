use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::domain::ids::{ResolutionId, RunId};
use crate::domain::job::status::JobStatus;
use crate::domain::wire::db_string_serde;
use crate::error::JobsError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub enum JobResolutionKind {
    Completed,
    Failed,
    Cancelled,
}

impl JobResolutionKind {
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
                field: "job_resolution_kind",
                value: other.to_owned(),
            }),
        }
    }

    pub fn as_status(&self) -> JobStatus {
        match self {
            Self::Completed => JobStatus::Completed,
            Self::Failed => JobStatus::Failed,
            Self::Cancelled => JobStatus::Cancelled,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub enum JobFailureCause {
    TerminalRunFailure,
    DeclaredByOwner,
    InactivityTimeout,
}

impl JobFailureCause {
    pub fn as_db_str(&self) -> &'static str {
        match self {
            Self::TerminalRunFailure => "TERMINAL_RUN_FAILURE",
            Self::DeclaredByOwner => "DECLARED_BY_OWNER",
            Self::InactivityTimeout => "INACTIVITY_TIMEOUT",
        }
    }

    pub fn from_db_str(value: &str) -> Result<Self, JobsError> {
        match value {
            "TERMINAL_RUN_FAILURE" => Ok(Self::TerminalRunFailure),
            "DECLARED_BY_OWNER" => Ok(Self::DeclaredByOwner),
            "INACTIVITY_TIMEOUT" => Ok(Self::InactivityTimeout),
            other => Err(JobsError::UnknownEnumValue {
                field: "job_failure_cause",
                value: other.to_owned(),
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobResolution {
    id: ResolutionId,
    kind: JobResolutionKind,
    occurred_at: DateTime<Utc>,
    failure_cause: Option<JobFailureCause>,
    caused_by_run_id: Option<RunId>,
}

impl JobResolution {
    pub fn completed(id: ResolutionId, occurred_at: DateTime<Utc>) -> Self {
        Self {
            id,
            kind: JobResolutionKind::Completed,
            occurred_at,
            failure_cause: None,
            caused_by_run_id: None,
        }
    }

    pub fn cancelled(id: ResolutionId, occurred_at: DateTime<Utc>) -> Self {
        Self {
            id,
            kind: JobResolutionKind::Cancelled,
            occurred_at,
            failure_cause: None,
            caused_by_run_id: None,
        }
    }

    pub fn failed(
        id: ResolutionId,
        occurred_at: DateTime<Utc>,
        failure_cause: JobFailureCause,
        caused_by_run_id: Option<RunId>,
    ) -> Result<Self, JobsError> {
        if failure_cause == JobFailureCause::TerminalRunFailure && caused_by_run_id.is_none() {
            return Err(JobsError::CorruptState {
                reason_code: "terminal_run_failure_without_run",
            });
        }
        Ok(Self {
            id,
            kind: JobResolutionKind::Failed,
            occurred_at,
            failure_cause: Some(failure_cause),
            caused_by_run_id,
        })
    }

    pub fn hydrate(
        id: ResolutionId,
        kind: JobResolutionKind,
        occurred_at: DateTime<Utc>,
        failure_cause: Option<JobFailureCause>,
        caused_by_run_id: Option<RunId>,
    ) -> Result<Self, JobsError> {
        match (kind, failure_cause) {
            (JobResolutionKind::Failed, Some(cause)) => {
                Self::failed(id, occurred_at, cause, caused_by_run_id)
            }
            (JobResolutionKind::Failed, None) => Err(JobsError::CorruptState {
                reason_code: "failed_job_without_cause",
            }),
            (_, Some(_)) => Err(JobsError::CorruptState {
                reason_code: "non_failed_job_with_cause",
            }),
            (JobResolutionKind::Completed, None) => Ok(Self::completed(id, occurred_at)),
            (JobResolutionKind::Cancelled, None) => Ok(Self::cancelled(id, occurred_at)),
        }
    }

    pub fn id(&self) -> ResolutionId {
        self.id
    }

    pub fn kind(&self) -> JobResolutionKind {
        self.kind
    }

    pub fn occurred_at(&self) -> DateTime<Utc> {
        self.occurred_at
    }

    pub fn failure_cause(&self) -> Option<JobFailureCause> {
        self.failure_cause
    }

    pub fn caused_by_run_id(&self) -> Option<RunId> {
        self.caused_by_run_id
    }

    pub fn is_failure(&self) -> bool {
        self.kind == JobResolutionKind::Failed
    }
}

db_string_serde!(JobResolutionKind);

db_string_serde!(JobFailureCause);

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn resolution_id() -> ResolutionId {
        ResolutionId::new(Uuid::now_v7()).unwrap()
    }

    fn at() -> DateTime<Utc> {
        DateTime::from_timestamp(1_700_000_000, 0).unwrap()
    }

    #[test]
    fn a_run_caused_failure_must_name_the_run_that_caused_it() {
        // Given: a failure attributed to a terminal run
        let result = JobResolution::failed(
            resolution_id(),
            at(),
            JobFailureCause::TerminalRunFailure,
            None,
        );
        // Then: the resolution is refused without the causing run
        assert_eq!(
            result,
            Err(JobsError::CorruptState {
                reason_code: "terminal_run_failure_without_run"
            })
        );
    }

    #[test]
    fn an_owner_declared_failure_needs_no_run() {
        // Given: an owner judging the work unrecoverable
        let resolution = JobResolution::failed(
            resolution_id(),
            at(),
            JobFailureCause::DeclaredByOwner,
            None,
        )
        .unwrap();
        // Then: the cause is recorded and the job reads as failed
        assert_eq!(
            resolution.failure_cause(),
            Some(JobFailureCause::DeclaredByOwner)
        );
        assert_eq!(resolution.kind().as_status(), JobStatus::Failed);
    }

    #[test]
    fn a_cancelled_resolution_carrying_a_failure_cause_cannot_be_loaded() {
        // Given: a stored CANCELLED resolution that also names a failure cause
        let result = JobResolution::hydrate(
            resolution_id(),
            JobResolutionKind::Cancelled,
            at(),
            Some(JobFailureCause::DeclaredByOwner),
            None,
        );
        // Then: the contradiction is refused at load
        assert_eq!(
            result,
            Err(JobsError::CorruptState {
                reason_code: "non_failed_job_with_cause"
            })
        );
    }

    #[test]
    fn a_failed_resolution_without_a_cause_cannot_be_loaded() {
        // Given: a stored FAILED resolution with no cause
        let result =
            JobResolution::hydrate(resolution_id(), JobResolutionKind::Failed, at(), None, None);
        // Then: it is refused — every failed job records exactly one cause
        assert_eq!(
            result,
            Err(JobsError::CorruptState {
                reason_code: "failed_job_without_cause"
            })
        );
    }
}
