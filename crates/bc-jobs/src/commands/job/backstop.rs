use chrono::{DateTime, TimeDelta, Utc};

use crate::commands::job::run_outcome::RunFailureFact;
use crate::commands::{CommandResult, CommandWarning, JobCommandResult};
use crate::domain::ids::{ResolutionId, RetryScheduleId, RunId};
use crate::domain::job::Job;
use crate::domain::job::resolution::JobFailureCause;
use crate::domain::job::status::JobStatus;
use crate::domain::keys::ReasonCode;
use crate::domain::policy::{Jitter, RetryPolicy, ServiceLimits};
use crate::domain::run::failure::{
    CLAIM_LOST, INSTANCE_LOST, RUN_TIMEOUT, RunFailureKind, RunFailureReport, TRIGGER_STRANDED,
};
use crate::error::JobsError;
use crate::event::job::JobEvent;
use crate::event::job_facts::{JobFailed, RunCancellationRequested, RunCancelled};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReclaimRun {
    pub run_id: RunId,
    pub retry_schedule_id: RetryScheduleId,
    pub resolution_id: ResolutionId,
    pub jitter: Jitter,
    pub at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WithdrawStrandedTrigger {
    pub run_id: RunId,
    pub at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailForInactivity {
    pub resolution_id: ResolutionId,
    pub at: DateTime<Utc>,
}

impl ReclaimRun {
    fn into_failure(self, report: RunFailureReport) -> RunFailureFact {
        RunFailureFact {
            run_id: self.run_id,
            report,
            retry_schedule_id: self.retry_schedule_id,
            resolution_id: self.resolution_id,
            jitter: self.jitter,
            at: self.at,
        }
    }
}

impl Job {
    pub fn fail_run_for_instance_loss(
        &self,
        command: ReclaimRun,
        policy: &RetryPolicy,
        limits: &ServiceLimits,
    ) -> Result<JobCommandResult, JobsError> {
        let run = self.find_run(command.run_id)?;
        if let Some(discarded) = self.discard_settled("RunFailed", run) {
            return Ok(discarded);
        }
        let report = RunFailureReport::platform(RunFailureKind::Transient, INSTANCE_LOST)?;
        let fact = command.into_failure(report);
        Ok(CommandResult::new(
            self.run_failure_events(run, &fact, policy, limits)?,
        ))
    }

    pub fn fail_run_for_max_duration(
        &self,
        command: ReclaimRun,
        policy: &RetryPolicy,
        limits: &ServiceLimits,
    ) -> Result<JobCommandResult, JobsError> {
        let run = self.find_run(command.run_id)?;
        if let Some(discarded) = self.discard_settled("RunFailed", run) {
            return Ok(discarded);
        }
        if !run.has_outrun(command.at, limits.max_run_duration()) {
            return Err(JobsError::RunWithinMaxDuration {
                run_id: run.id().as_uuid(),
            });
        }
        let mut events = vec![JobEvent::RunCancellationRequested(
            RunCancellationRequested {
                job_id: self.id(),
                run_id: run.id(),
                reason_code: ReasonCode::new(RUN_TIMEOUT)?,
                requested_by: None,
                originating_job_id: None,
            },
        )];
        let report = RunFailureReport::platform(RunFailureKind::Transient, RUN_TIMEOUT)?;
        let fact = command.into_failure(report);
        events.extend(self.run_failure_events(run, &fact, policy, limits)?);
        Ok(CommandResult::new(events).with_warning(CommandWarning::CancellationIsBestEffort))
    }

    pub fn fail_run_for_lost_claim(
        &self,
        command: ReclaimRun,
        reservation_delay: TimeDelta,
        policy: &RetryPolicy,
        limits: &ServiceLimits,
    ) -> Result<JobCommandResult, JobsError> {
        let run = self.find_run(command.run_id)?;
        if let Some(discarded) = self.discard_settled("RunFailed", run) {
            return Ok(discarded);
        }
        if !run.is_reserved() {
            return Err(JobsError::RunNotReserved {
                run_id: run.id().as_uuid(),
            });
        }
        if run.has_started() {
            return Ok(CommandResult::nothing_happened(
                CommandWarning::FactAlreadyRecorded { fact: "RunStarted" },
            ));
        }
        if !run.has_lost_its_claim(command.at, reservation_delay) {
            return Err(JobsError::ReservationWithinDelay {
                run_id: run.id().as_uuid(),
            });
        }
        let mut events = vec![JobEvent::RunCancellationRequested(
            RunCancellationRequested {
                job_id: self.id(),
                run_id: run.id(),
                reason_code: ReasonCode::new(CLAIM_LOST)?,
                requested_by: None,
                originating_job_id: None,
            },
        )];
        let report = RunFailureReport::platform(RunFailureKind::Transient, CLAIM_LOST)?;
        let fact = command.into_failure(report);
        events.extend(self.run_failure_events(run, &fact, policy, limits)?);
        Ok(CommandResult::new(events).with_warning(CommandWarning::CancellationIsBestEffort))
    }

    pub fn withdraw_stranded_trigger(
        &self,
        command: WithdrawStrandedTrigger,
        claimed_since: Option<DateTime<Utc>>,
        delay: TimeDelta,
    ) -> Result<JobCommandResult, JobsError> {
        let run = self.find_run(command.run_id)?;
        if let Some(discarded) = self.discard_settled("RunCancelled", run) {
            return Ok(discarded);
        }
        let Some(claimed_since) = claimed_since else {
            return Err(JobsError::NotClaimMode {
                runner_type: self.runner_type().as_str().to_owned(),
            });
        };
        if !run.has_a_stranded_trigger(command.at, delay, claimed_since) {
            return Err(JobsError::TriggerNotStranded {
                run_id: run.id().as_uuid(),
            });
        }
        let reason = ReasonCode::new(TRIGGER_STRANDED)?;
        Ok(CommandResult::new(vec![
            JobEvent::RunCancellationRequested(RunCancellationRequested {
                job_id: self.id(),
                run_id: run.id(),
                reason_code: reason.clone(),
                requested_by: None,
                originating_job_id: None,
            }),
            JobEvent::RunCancelled(RunCancelled {
                job_id: self.id(),
                run_id: run.id(),
                withdrawn_for: (!self.is_terminal()).then_some(reason),
            }),
        ])
        .with_warning(CommandWarning::CancellationIsBestEffort))
    }

    pub fn fail_for_inactivity(
        &self,
        command: FailForInactivity,
        limits: &ServiceLimits,
    ) -> Result<JobCommandResult, JobsError> {
        self.guard_not_deleted()?;
        if self.status() != JobStatus::InProgress {
            return Err(JobsError::JobNotInProgress {
                status: self.status().as_db_str(),
            });
        }
        if self.active_run().is_some() || self.unconsumed_retry().is_some() {
            return Err(JobsError::JobStillActive);
        }
        let idle_since = self.last_activity_at();
        if command.at - idle_since < limits.inactivity_timeout() {
            return Err(JobsError::InactivityTimeoutNotReached { idle_since });
        }
        Ok(CommandResult::from_event(JobEvent::JobFailed(JobFailed {
            job_id: self.id(),
            resolution_id: command.resolution_id,
            failure_cause: JobFailureCause::InactivityTimeout,
            caused_by_run_id: None,
            report: None,
        })))
    }
}

#[cfg(test)]
mod tests;
