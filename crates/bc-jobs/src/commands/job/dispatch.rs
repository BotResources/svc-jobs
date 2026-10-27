use chrono::{DateTime, Utc};

use crate::commands::{CommandResult, JobCommandResult};
use crate::domain::attempts::AttemptNumber;
use crate::domain::fleet::run_duration::MaxRunDuration;
use crate::domain::ids::{ClaimId, RunId};
use crate::domain::job::Job;
use crate::domain::policy::ServiceLimits;
use crate::domain::run::Run;
use crate::domain::run::origin::RunOrigin;
use crate::domain::run::parts::RunnerInstanceReference;
use crate::error::JobsError;
use crate::event::job::JobEvent;
use crate::event::job_facts::{RunClaim, RunDispatched};

/// Publishing the next Run of a waiting Job on the trigger stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DispatchRun {
    pub run_id: RunId,
    pub at: DateTime<Utc>,
    /// The maximum duration of the Job's RunnerType, carried by the Run.
    pub max_run_duration: MaxRunDuration,
}

/// Handing the next Run of a waiting Job to the Instance that claimed it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReserveRun {
    pub run_id: RunId,
    pub at: DateTime<Utc>,
    pub max_run_duration: MaxRunDuration,
    pub instance: RunnerInstanceReference,
    pub claim_id: ClaimId,
}

impl Job {
    pub fn dispatch_run(
        &self,
        command: DispatchRun,
        limits: &ServiceLimits,
    ) -> Result<JobCommandResult, JobsError> {
        self.next_run(
            command.run_id,
            command.at,
            command.max_run_duration,
            None,
            limits,
        )
    }

    /// The same decision as a push dispatch, plus the claimant: a claim is
    /// served from the very Jobs a push would publish, in the same order.
    pub fn reserve_run(
        &self,
        command: ReserveRun,
        limits: &ServiceLimits,
    ) -> Result<JobCommandResult, JobsError> {
        if command.instance.runner_type() != self.runner_type() {
            return Err(JobsError::RunnerTypeMismatch {
                expected: self.runner_type().as_str().to_owned(),
                claimed: command.instance.runner_type().as_str().to_owned(),
            });
        }
        self.next_run(
            command.run_id,
            command.at,
            command.max_run_duration,
            Some(RunClaim {
                instance_key: command.instance.instance_key().clone(),
                claim_id: command.claim_id,
            }),
            limits,
        )
    }

    fn next_run(
        &self,
        run_id: RunId,
        at: DateTime<Utc>,
        max_run_duration: MaxRunDuration,
        claimed_by: Option<RunClaim>,
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
        let retried = self.due_retry(attempt_number, at)?;
        Ok(CommandResult::from_event(JobEvent::RunDispatched(
            RunDispatched {
                job_id: self.id(),
                run_id,
                runner_type: self.runner_type().clone(),
                attempt_number,
                origin: RunOrigin::derive(attempt_number.is_first(), self.has_predecessor()),
                automatic_retry_schedule_id: retried
                    .and_then(Run::retry_schedule)
                    .map(|schedule| schedule.id()),
                automatic_retry_of_run_id: retried.map(Run::id),
                max_run_duration: Some(max_run_duration),
                claimed_by,
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
                    max_run_duration: MaxRunDuration::SERVICE_DEFAULT,
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
                    max_run_duration: MaxRunDuration::SERVICE_DEFAULT,
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
                max_run_duration: MaxRunDuration::SERVICE_DEFAULT,
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
                max_run_duration: MaxRunDuration::SERVICE_DEFAULT,
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
                    max_run_duration: MaxRunDuration::SERVICE_DEFAULT,
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
    fn a_retry_whose_trigger_was_withdrawn_goes_out_again_as_the_same_attempt() {
        // Given: attempt one failed with a retry due at ts(80), and the retry's
        // trigger was withdrawn as stranded
        let failed = RunBuilder::new(1)
            .started(ts(5))
            .failed(ts(20), RunFailureKind::Transient)
            .retry_due(ts(80))
            .build();
        let schedule = failed.retry_schedule().unwrap().id();
        let withdrawn = RunBuilder::new(2)
            .retry_of_schedule(schedule)
            .withdrawn(ts(150), "trigger_stranded")
            .build();
        let job = JobBuilder::new()
            .with_run(failed)
            .with_run(withdrawn)
            .build();
        // Then: the job waits again, one attempt spent
        assert!(job.is_waiting());
        assert_eq!(job.attempt_count(), 1);
        // When: the retry is dispatched again
        let fact = dispatched(
            job.dispatch_run(
                DispatchRun {
                    run_id: run_id(),
                    at: ts(160),
                    max_run_duration: MaxRunDuration::SERVICE_DEFAULT,
                },
                &ServiceLimits::default(),
            )
            .unwrap(),
        );
        // Then: it is attempt two again, consuming the same schedule
        assert_eq!(fact.attempt_number.get(), 2);
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
                max_run_duration: MaxRunDuration::SERVICE_DEFAULT,
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
                max_run_duration: MaxRunDuration::SERVICE_DEFAULT,
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
                max_run_duration: MaxRunDuration::SERVICE_DEFAULT,
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

    fn reserve(
        job: &Job,
        instance: RunnerInstanceReference,
    ) -> Result<JobCommandResult, JobsError> {
        job.reserve_run(
            ReserveRun {
                run_id: run_id(),
                at: ts(10),
                max_run_duration: MaxRunDuration::from_seconds(96 * 3600).unwrap(),
                instance,
                claim_id: crate::fixtures::claim_id(),
            },
            &ServiceLimits::default(),
        )
    }

    #[test]
    fn a_claim_reserves_the_first_run_to_the_claiming_instance() {
        // Given: a waiting job of a CLAIM type
        let job = JobBuilder::new().build();
        // When: pod-7 claims it
        let fact = dispatched(reserve(&job, crate::fixtures::instance()).unwrap());
        // Then: attempt one is created reserved to pod-7, with its type's maximum duration
        assert_eq!(fact.attempt_number, AttemptNumber::FIRST);
        assert_eq!(
            fact.claimed_by.map(|claim| claim.instance_key),
            Some(crate::fixtures::instance().instance_key().clone())
        );
        assert_eq!(
            fact.max_run_duration,
            Some(MaxRunDuration::from_seconds(96 * 3600).unwrap())
        );
    }

    #[test]
    fn a_pushed_run_is_reserved_to_nobody_and_carries_its_types_duration() {
        // Given: a waiting job of a PUSH type
        let job = JobBuilder::new().build();
        // When: it is dispatched
        let fact = dispatched(
            job.dispatch_run(
                DispatchRun {
                    run_id: run_id(),
                    at: ts(10),
                    max_run_duration: MaxRunDuration::from_seconds(600).unwrap(),
                },
                &ServiceLimits::default(),
            )
            .unwrap(),
        );
        // Then: no claimant, and the duration the run will be held to
        assert_eq!(fact.claimed_by, None);
        assert_eq!(
            fact.max_run_duration.map(MaxRunDuration::seconds),
            Some(600)
        );
    }

    #[test]
    fn a_claim_is_refused_whatever_the_push_dispatch_would_refuse() {
        // Given: a job whose run is already in flight
        let live = RunBuilder::new(1).reserved(ts(1)).build();
        let live_id = live.id();
        let job = JobBuilder::new().with_run(live).build();
        // When: another claim reaches it
        let result = reserve(&job, crate::fixtures::instance());
        // Then: the same refusal as a push dispatch — one decision, two transports
        assert_eq!(
            result,
            Err(JobsError::RunAlreadyInFlight {
                run_id: live_id.as_uuid()
            })
        );
    }

    #[test]
    fn an_instance_of_another_type_may_not_claim_the_job() {
        // Given: an analyst job, and a scribe instance claiming it
        let job = JobBuilder::new().build();
        let scribe = RunnerInstanceReference::new(
            crate::domain::keys::RunnerTypeKey::new("scribe").unwrap(),
            crate::domain::keys::InstanceKey::new("pod-7").unwrap(),
        );
        // When/Then: the claim is refused
        assert_eq!(
            reserve(&job, scribe),
            Err(JobsError::RunnerTypeMismatch {
                expected: "analyst".to_owned(),
                claimed: "scribe".to_owned(),
            })
        );
    }
}
