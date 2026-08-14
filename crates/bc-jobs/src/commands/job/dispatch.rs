use chrono::{DateTime, Utc};

use crate::commands::{CommandResult, JobCommandResult};
use crate::domain::attempts::AttemptNumber;
use crate::domain::ids::RunId;
use crate::domain::job::Job;
use crate::domain::policy::ServiceLimits;
use crate::domain::run::Run;
use crate::domain::run::origin::RunOrigin;
use crate::error::JobsError;
use crate::event::job::JobEvent;
use crate::event::job_facts::RunDispatched;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DispatchRun {
    pub run_id: RunId,
    pub at: DateTime<Utc>,
}

impl Job {
    pub fn dispatch_run(
        &self,
        command: DispatchRun,
        limits: &ServiceLimits,
    ) -> Result<JobCommandResult, JobsError> {
        self.guard_not_deleted()?;
        self.guard_not_terminal()?;
        if let Some(active) = self.active_run() {
            return Err(JobsError::RunAlreadyInFlight {
                run_id: active.id().as_uuid(),
            });
        }
        let attempt_number = AttemptNumber::new(self.attempt_count().saturating_add(1))?;
        let budget = self.budget(limits)?;
        if !budget.allows(attempt_number) {
            return Err(JobsError::RetryBudgetExhausted {
                attempts: self.attempt_count(),
                max_attempts: budget.get(),
            });
        }
        let retried = self.due_retry(attempt_number, command.at)?;
        Ok(CommandResult::from_event(JobEvent::RunDispatched(
            RunDispatched {
                job_id: self.id(),
                run_id: command.run_id,
                runner_type: self.runner_type().clone(),
                attempt_number,
                origin: RunOrigin::derive(attempt_number.is_first(), self.has_predecessor()),
                automatic_retry_schedule_id: retried
                    .and_then(Run::retry_schedule)
                    .map(|schedule| schedule.id()),
                automatic_retry_of_run_id: retried.map(Run::id),
            },
        )))
    }

    fn due_retry(
        &self,
        attempt_number: AttemptNumber,
        at: DateTime<Utc>,
    ) -> Result<Option<&Run>, JobsError> {
        if attempt_number.is_first() {
            return Ok(None);
        }
        let failed = self
            .unconsumed_retry()
            .ok_or(JobsError::RetryNotScheduled)?;
        let schedule = failed
            .retry_schedule()
            .ok_or(JobsError::RetryNotScheduled)?;
        if at < schedule.due_at() {
            return Err(JobsError::RetryNotDue {
                due_at: schedule.due_at(),
            });
        }
        Ok(Some(failed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::job::resolution::JobResolution;
    use crate::domain::run::failure::RunFailureKind;
    use crate::fixtures::{JobBuilder, RunBuilder, job_id, resolution_id, run_id, ts};

    fn dispatched(result: JobCommandResult) -> RunDispatched {
        match result.events.into_iter().next() {
            Some(JobEvent::RunDispatched(fact)) => fact,
            other => panic!("expected a RunDispatched fact, got {other:?}"),
        }
    }

    fn failed_first_attempt(due_at: chrono::DateTime<Utc>) -> JobBuilder {
        JobBuilder::new().with_run(
            RunBuilder::new(1)
                .started(ts(5))
                .failed(ts(20), RunFailureKind::Transient)
                .retry_due(due_at)
                .build(),
        )
    }

    #[test]
    fn the_first_dispatch_of_a_pending_job_is_attempt_one() {
        // Given: a queued job with no attempt yet
        let job = JobBuilder::new().build();
        // When: the dispatcher sends it out
        let result = job
            .dispatch_run(
                DispatchRun {
                    run_id: run_id(),
                    at: ts(10),
                },
                &ServiceLimits::default(),
            )
            .unwrap();
        // Then: attempt one is dispatched as the initial run, consuming no schedule
        let fact = dispatched(result);
        assert_eq!(fact.attempt_number, AttemptNumber::FIRST);
        assert_eq!(fact.origin, RunOrigin::Initial);
        assert_eq!(fact.automatic_retry_of_run_id, None);
        assert_eq!(fact.automatic_retry_schedule_id, None);
    }

    #[test]
    fn the_first_run_of_a_manual_retry_successor_is_attributed_to_the_intervention() {
        // Given: a successor job created by an administrator's manual retry
        let job = JobBuilder::new().with_predecessor(job_id()).build();
        // When: its first run goes out
        let result = job
            .dispatch_run(
                DispatchRun {
                    run_id: run_id(),
                    at: ts(10),
                },
                &ServiceLimits::default(),
            )
            .unwrap();
        // Then: the run is attributed to the manual retry, not to the platform
        assert_eq!(dispatched(result).origin, RunOrigin::ManualRetry);
    }

    #[test]
    fn a_second_run_may_not_be_dispatched_while_one_is_in_flight() {
        // Given: a job with a live attempt
        let live = RunBuilder::new(1).started(ts(5)).build();
        let live_id = live.id();
        let job = JobBuilder::new().with_run(live).build();
        // When: a second dispatch is attempted
        let result = job.dispatch_run(
            DispatchRun {
                run_id: run_id(),
                at: ts(10),
            },
            &ServiceLimits::default(),
        );
        // Then: it is refused, naming the run already in flight
        assert_eq!(
            result,
            Err(JobsError::RunAlreadyInFlight {
                run_id: live_id.as_uuid()
            })
        );
    }

    #[test]
    fn a_retry_dispatched_before_its_due_time_is_refused() {
        // Given: a failed attempt whose next try is due at plus eighty seconds
        let job = failed_first_attempt(ts(80)).build();
        // When: the dispatcher tries at plus fifty
        let result = job.dispatch_run(
            DispatchRun {
                run_id: run_id(),
                at: ts(50),
            },
            &ServiceLimits::default(),
        );
        // Then: the recorded due time governs — the delay is never recomputed away
        assert_eq!(result, Err(JobsError::RetryNotDue { due_at: ts(80) }));
    }

    #[test]
    fn a_retry_dispatched_at_its_due_time_consumes_its_schedule() {
        // Given: a failed attempt whose retry is due
        let job = failed_first_attempt(ts(80)).build();
        let failed_run = job.latest_run().unwrap();
        let schedule = failed_run.retry_schedule().unwrap().id();
        let failed_id = failed_run.id();
        // When: the dispatcher sends the retry at the due time
        let result = job
            .dispatch_run(
                DispatchRun {
                    run_id: run_id(),
                    at: ts(80),
                },
                &ServiceLimits::default(),
            )
            .unwrap();
        // Then: attempt two goes out as an automatic retry of the failed run
        let fact = dispatched(result);
        assert_eq!(fact.attempt_number.get(), 2);
        assert_eq!(fact.origin, RunOrigin::AutomaticRetry);
        assert_eq!(fact.automatic_retry_of_run_id, Some(failed_id));
        assert_eq!(fact.automatic_retry_schedule_id, Some(schedule));
    }

    #[test]
    fn a_retry_with_no_schedule_behind_it_is_refused() {
        // Given: a job whose first attempt failed without a retry being scheduled
        let job = JobBuilder::new()
            .with_run(
                RunBuilder::new(1)
                    .started(ts(5))
                    .failed(ts(20), RunFailureKind::Permanent)
                    .build(),
            )
            .build();
        // When: a second attempt is dispatched anyway
        let result = job.dispatch_run(
            DispatchRun {
                run_id: run_id(),
                at: ts(90),
            },
            &ServiceLimits::default(),
        );
        // Then: it is refused — only a scheduled retry may be dispatched
        assert_eq!(result, Err(JobsError::RetryNotScheduled));
    }

    #[test]
    fn dispatch_stops_when_the_attempt_budget_is_exhausted() {
        // Given: a job allowed one attempt, which already failed and was rescheduled
        let job = failed_first_attempt(ts(80)).with_max_attempts(1).build();
        // When: the retry is dispatched
        let result = job.dispatch_run(
            DispatchRun {
                run_id: run_id(),
                at: ts(80),
            },
            &ServiceLimits::default(),
        );
        // Then: the budget refuses it, carrying both figures
        assert_eq!(
            result,
            Err(JobsError::RetryBudgetExhausted {
                attempts: 1,
                max_attempts: 1
            })
        );
    }

    #[test]
    fn a_terminal_job_dispatches_nothing_more() {
        // Given: a job that already resolved
        let job = JobBuilder::new()
            .with_resolution(JobResolution::cancelled(resolution_id(), ts(30)))
            .build();
        // When: a dispatch is attempted
        let result = job.dispatch_run(
            DispatchRun {
                run_id: run_id(),
                at: ts(40),
            },
            &ServiceLimits::default(),
        );
        // Then: a terminal job never becomes active again
        assert_eq!(
            result,
            Err(JobsError::JobAlreadyTerminal {
                status: "CANCELLED"
            })
        );
    }
}
