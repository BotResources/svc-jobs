use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::domain::ids::{JobId, RetryScheduleId};
use crate::domain::keys::{InstanceKey, ReasonCode, RunnerTypeKey};
use crate::domain::references::KnownUser;
use crate::domain::run::failure::RunFailureReport;
use crate::domain::run::status::RunTerminalKind;
use crate::error::JobsError;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunnerInstanceReference {
    runner_type: RunnerTypeKey,
    instance_key: InstanceKey,
}

impl RunnerInstanceReference {
    pub fn new(runner_type: RunnerTypeKey, instance_key: InstanceKey) -> Self {
        Self {
            runner_type,
            instance_key,
        }
    }

    pub fn runner_type(&self) -> &RunnerTypeKey {
        &self.runner_type
    }

    pub fn instance_key(&self) -> &InstanceKey {
        &self.instance_key
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunStart {
    instance: RunnerInstanceReference,
    started_at: DateTime<Utc>,
}

impl RunStart {
    pub fn new(instance: RunnerInstanceReference, started_at: DateTime<Utc>) -> Self {
        Self {
            instance,
            started_at,
        }
    }

    pub fn instance(&self) -> &RunnerInstanceReference {
        &self.instance
    }

    pub fn started_at(&self) -> DateTime<Utc> {
        self.started_at
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunTerminal {
    kind: RunTerminalKind,
    occurred_at: DateTime<Utc>,
    failure: Option<RunFailureReport>,
}

impl RunTerminal {
    pub fn completed(occurred_at: DateTime<Utc>) -> Self {
        Self {
            kind: RunTerminalKind::Completed,
            occurred_at,
            failure: None,
        }
    }

    pub fn cancelled(occurred_at: DateTime<Utc>) -> Self {
        Self {
            kind: RunTerminalKind::Cancelled,
            occurred_at,
            failure: None,
        }
    }

    pub fn failed(occurred_at: DateTime<Utc>, failure: RunFailureReport) -> Self {
        Self {
            kind: RunTerminalKind::Failed,
            occurred_at,
            failure: Some(failure),
        }
    }

    pub fn hydrate(
        kind: RunTerminalKind,
        occurred_at: DateTime<Utc>,
        failure: Option<RunFailureReport>,
    ) -> Result<Self, JobsError> {
        match (kind, failure) {
            (RunTerminalKind::Failed, Some(report)) => Ok(Self::failed(occurred_at, report)),
            (RunTerminalKind::Failed, None) => Err(JobsError::CorruptState {
                reason_code: "failed_run_without_report",
            }),
            (_, Some(_)) => Err(JobsError::CorruptState {
                reason_code: "non_failed_run_with_report",
            }),
            (RunTerminalKind::Completed, None) => Ok(Self::completed(occurred_at)),
            (RunTerminalKind::Cancelled, None) => Ok(Self::cancelled(occurred_at)),
        }
    }

    pub fn kind(&self) -> RunTerminalKind {
        self.kind
    }

    pub fn occurred_at(&self) -> DateTime<Utc> {
        self.occurred_at
    }

    pub fn failure(&self) -> Option<&RunFailureReport> {
        self.failure.as_ref()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetrySchedule {
    id: RetryScheduleId,
    due_at: DateTime<Utc>,
}

impl RetrySchedule {
    pub fn new(id: RetryScheduleId, due_at: DateTime<Utc>) -> Self {
        Self { id, due_at }
    }

    pub fn id(&self) -> RetryScheduleId {
        self.id
    }

    pub fn due_at(&self) -> DateTime<Utc> {
        self.due_at
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunCancellationRequest {
    requested_at: DateTime<Utc>,
    reason_code: ReasonCode,
    requested_by: Option<KnownUser>,
    originating_job_id: Option<JobId>,
}

impl RunCancellationRequest {
    pub fn new(
        requested_at: DateTime<Utc>,
        reason_code: ReasonCode,
        requested_by: Option<KnownUser>,
        originating_job_id: Option<JobId>,
    ) -> Self {
        Self {
            requested_at,
            reason_code,
            requested_by,
            originating_job_id,
        }
    }

    pub fn requested_at(&self) -> DateTime<Utc> {
        self.requested_at
    }

    pub fn reason_code(&self) -> &ReasonCode {
        &self.reason_code
    }

    pub fn requested_by(&self) -> Option<&KnownUser> {
        self.requested_by.as_ref()
    }

    pub fn originating_job_id(&self) -> Option<JobId> {
        self.originating_job_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at() -> DateTime<Utc> {
        DateTime::from_timestamp(1_700_000_000, 0).unwrap()
    }

    #[test]
    fn a_failed_terminal_without_its_report_cannot_be_loaded() {
        // Given: a stored FAILED run whose report row is missing
        let result = RunTerminal::hydrate(RunTerminalKind::Failed, at(), None);
        // Then: the run refuses to load rather than serving a reportless failure
        assert_eq!(
            result,
            Err(JobsError::CorruptState {
                reason_code: "failed_run_without_report"
            })
        );
    }

    #[test]
    fn a_completed_terminal_carrying_a_failure_report_cannot_be_loaded() {
        // Given: a stored COMPLETED run that somehow carries a failure report
        let report = RunFailureReport::platform(
            crate::domain::run::failure::RunFailureKind::Transient,
            "instance_lost",
        )
        .unwrap();
        let result = RunTerminal::hydrate(RunTerminalKind::Completed, at(), Some(report));
        // Then: the contradiction is refused at load time
        assert_eq!(
            result,
            Err(JobsError::CorruptState {
                reason_code: "non_failed_run_with_report"
            })
        );
    }
}
