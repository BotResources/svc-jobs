use chrono::{DateTime, Utc};

use crate::commands::{CommandResult, JobCommandResult};
use crate::domain::attempts::AttemptNumber;
use crate::domain::ids::{ResolutionId, RetryScheduleId, RunId};
use crate::domain::job::Job;
use crate::domain::job::resolution::JobFailureCause;
use crate::domain::policy::{Jitter, RetryPolicy, ServiceLimits};
use crate::domain::run::Run;
use crate::domain::run::failure::RunFailureReport;
use crate::error::JobsError;
use crate::event::job::JobEvent;
use crate::event::job_facts::{JobFailed, RetryScheduled, RunCompleted, RunFailed};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunCompletedFact {
    pub run_id: RunId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunFailureFact {
    pub run_id: RunId,
    pub report: RunFailureReport,
    pub retry_schedule_id: RetryScheduleId,
    pub resolution_id: ResolutionId,
    pub jitter: Jitter,
    pub at: DateTime<Utc>,
}

fn guard_started(run: &Run) -> Result<(), JobsError> {
    if run.has_started() {
        Ok(())
    } else {
        Err(JobsError::RunNotStarted {
            run_id: run.id().as_uuid(),
        })
    }
}

impl Job {
    pub fn record_run_completed(
        &self,
        fact: RunCompletedFact,
    ) -> Result<JobCommandResult, JobsError> {
        if let Some(discarded) = self.discard_on_terminal_job("RunCompleted") {
            return Ok(discarded);
        }
        let run = self.find_run(fact.run_id)?;
        if let Some(discarded) = self.discard_settled("RunCompleted", run) {
            return Ok(discarded);
        }
        guard_started(run)?;
        Ok(CommandResult::from_event(JobEvent::RunCompleted(
            RunCompleted {
                job_id: self.id(),
                run_id: run.id(),
                attempt_number: run.attempt_number(),
            },
        )))
    }

    pub fn record_run_failed(
        &self,
        fact: RunFailureFact,
        policy: &RetryPolicy,
        limits: &ServiceLimits,
    ) -> Result<JobCommandResult, JobsError> {
        if let Some(discarded) = self.discard_on_terminal_job("RunFailed") {
            return Ok(discarded);
        }
        let run = self.find_run(fact.run_id)?;
        if let Some(discarded) = self.discard_settled("RunFailed", run) {
            return Ok(discarded);
        }
        guard_started(run)?;
        Ok(CommandResult::new(
            self.run_failure_events(run, &fact, policy, limits)?,
        ))
    }

    pub(crate) fn run_failure_events(
        &self,
        run: &Run,
        fact: &RunFailureFact,
        policy: &RetryPolicy,
        limits: &ServiceLimits,
    ) -> Result<Vec<JobEvent>, JobsError> {
        let failed_attempt = run.attempt_number();
        let mut events = vec![JobEvent::RunFailed(RunFailed {
            job_id: self.id(),
            run_id: run.id(),
            attempt_number: failed_attempt,
            report: fact.report.clone(),
        })];
        let next_attempt_number = AttemptNumber::new(failed_attempt.get().saturating_add(1))?;
        let retryable =
            fact.report.kind().is_retryable() && self.budget(limits).allows(next_attempt_number);
        if retryable {
            events.push(JobEvent::RetryScheduled(RetryScheduled {
                job_id: self.id(),
                schedule_id: fact.retry_schedule_id,
                failed_run_id: run.id(),
                next_attempt_number,
                due_at: policy.due_at(
                    fact.at,
                    failed_attempt,
                    fact.jitter,
                    fact.report.retry_after(),
                ),
            }));
        } else {
            events.push(JobEvent::JobFailed(JobFailed {
                job_id: self.id(),
                resolution_id: fact.resolution_id,
                failure_cause: JobFailureCause::TerminalRunFailure,
                caused_by_run_id: Some(run.id()),
                report: Some(fact.report.clone()),
            }));
        }
        Ok(events)
    }
}

#[cfg(test)]
mod tests;
