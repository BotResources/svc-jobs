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
mod tests {
    use super::*;
    use crate::domain::run::failure::RunFailureKind;
    use crate::fixtures::{JobBuilder, RunBuilder, report, resolution_id, schedule_id, ts};

    fn failure(run_id: RunId, kind: RunFailureKind) -> RunFailureFact {
        RunFailureFact {
            run_id,
            report: report(kind),
            retry_schedule_id: schedule_id(),
            resolution_id: resolution_id(),
            jitter: Jitter::MIDPOINT,
            at: ts(20),
        }
    }

    #[test]
    fn a_successful_run_does_not_complete_its_job() {
        // Given: a job whose only attempt is running
        let run = RunBuilder::new(1).started(ts(5)).build();
        let run_id = run.id();
        let job = JobBuilder::new().with_run(run).build();
        // When: the runner reports success
        let result = job
            .record_run_completed(RunCompletedFact { run_id })
            .unwrap();
        // Then: only the run completes — the owner alone finishes the job
        assert_eq!(result.events.len(), 1);
        assert!(matches!(
            result.events.first(),
            Some(JobEvent::RunCompleted(_))
        ));
    }

    #[test]
    fn a_transient_failure_with_budget_left_schedules_another_attempt() {
        // Given: a running first attempt on a job allowed three attempts
        let run = RunBuilder::new(1).started(ts(5)).build();
        let run_id = run.id();
        let job = JobBuilder::new().with_run(run).build();
        // When: it fails transiently
        let result = job
            .record_run_failed(
                failure(run_id, RunFailureKind::Transient),
                &RetryPolicy::default(),
                &ServiceLimits::default(),
            )
            .unwrap();
        // Then: the failure is recorded and the next attempt gets a stored due time
        match result.events.as_slice() {
            [
                JobEvent::RunFailed(failed),
                JobEvent::RetryScheduled(scheduled),
            ] => {
                assert_eq!(failed.attempt_number, AttemptNumber::FIRST);
                assert_eq!(scheduled.failed_run_id, run_id);
                assert_eq!(scheduled.next_attempt_number.get(), 2);
                assert_eq!(scheduled.due_at, ts(30));
            }
            other => panic!("expected a failure and a retry, got {other:?}"),
        }
    }

    #[test]
    fn a_permanent_failure_ends_automatic_retry_immediately() {
        // Given: a running first attempt with budget to spare
        let run = RunBuilder::new(1).started(ts(5)).build();
        let run_id = run.id();
        let job = JobBuilder::new().with_run(run).build();
        // When: it fails permanently
        let result = job
            .record_run_failed(
                failure(run_id, RunFailureKind::Permanent),
                &RetryPolicy::default(),
                &ServiceLimits::default(),
            )
            .unwrap();
        // Then: the job fails at once, carrying the report to its owner
        match result.events.as_slice() {
            [JobEvent::RunFailed(_), JobEvent::JobFailed(job_failed)] => {
                assert_eq!(
                    job_failed.failure_cause,
                    JobFailureCause::TerminalRunFailure
                );
                assert_eq!(job_failed.caused_by_run_id, Some(run_id));
                assert!(job_failed.report.is_some());
            }
            other => panic!("expected a failure and a job failure, got {other:?}"),
        }
    }

    #[test]
    fn a_transient_failure_on_the_last_attempt_fails_the_job() {
        // Given: a job allowed one attempt, currently running it
        let run = RunBuilder::new(1).started(ts(5)).build();
        let run_id = run.id();
        let job = JobBuilder::new().with_max_attempts(1).with_run(run).build();
        // When: the attempt fails transiently
        let result = job
            .record_run_failed(
                failure(run_id, RunFailureKind::Transient),
                &RetryPolicy::default(),
                &ServiceLimits::default(),
            )
            .unwrap();
        // Then: the budget is spent, so the failure becomes terminal
        assert!(matches!(result.events.last(), Some(JobEvent::JobFailed(_))));
    }

    #[test]
    fn a_runner_retry_hint_lengthens_the_recorded_due_time() {
        // Given: a run failing transiently with a five minute hint
        let run = RunBuilder::new(1).started(ts(5)).build();
        let run_id = run.id();
        let job = JobBuilder::new().with_run(run).build();
        let hinted = RunFailureFact {
            report: RunFailureReport::new(
                RunFailureKind::Transient,
                crate::domain::keys::ReasonCode::new("provider_rate_limited").unwrap(),
                serde_json::json!({}),
                serde_json::json!({}),
                Some(chrono::TimeDelta::minutes(5)),
            )
            .unwrap(),
            ..failure(run_id, RunFailureKind::Transient)
        };
        // When: the failure is recorded
        let result = job
            .record_run_failed(hinted, &RetryPolicy::default(), &ServiceLimits::default())
            .unwrap();
        // Then: the hint wins over the shorter platform backoff
        match result.events.last() {
            Some(JobEvent::RetryScheduled(scheduled)) => {
                assert_eq!(scheduled.due_at, ts(20) + chrono::TimeDelta::minutes(5));
            }
            other => panic!("expected a retry, got {other:?}"),
        }
    }

    #[test]
    fn a_redelivered_terminal_fact_changes_no_history() {
        // Given: a run that already completed
        let run = RunBuilder::new(1).started(ts(5)).completed(ts(20)).build();
        let run_id = run.id();
        let job = JobBuilder::new().with_run(run).build();
        // When: a contradicting failure arrives for the same run
        let result = job
            .record_run_failed(
                failure(run_id, RunFailureKind::Transient),
                &RetryPolicy::default(),
                &ServiceLimits::default(),
            )
            .unwrap();
        // Then: the run never reopens — the fact is acknowledged and discarded
        assert!(result.is_empty());
    }
}
