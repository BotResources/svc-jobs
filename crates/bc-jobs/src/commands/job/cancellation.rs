use crate::commands::job::withdrawal::{CANCELLED_BY_JOB, RunWithdrawal};
use crate::commands::{CommandResult, CommandWarning, JobCommandResult};
use crate::domain::ids::{JobId, ResolutionId};
use crate::domain::job::Job;
use crate::domain::keys::ReasonCode;
use crate::domain::ownership::CancelRequester;
use crate::domain::references::KnownUser;
use crate::domain::run::Run;
use crate::error::JobsError;
use crate::event::job::JobEvent;
use crate::event::job_facts::JobCancelled;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancelJob {
    pub resolution_id: ResolutionId,
    pub requester: CancelRequester,
}

impl CancelRequester {
    fn known_user(&self) -> Option<&KnownUser> {
        match self {
            Self::Administrator(user) => Some(user),
            Self::Owner(_) | Self::Cascade { .. } => None,
        }
    }

    fn originating_job_id(&self) -> Option<JobId> {
        match self {
            Self::Cascade { originating_job_id } => Some(*originating_job_id),
            Self::Administrator(_) | Self::Owner(_) => None,
        }
    }
}

impl Job {
    pub fn cancel(&self, command: CancelJob) -> Result<JobCommandResult, JobsError> {
        if let Some(absorbed) = self.already_resolved_by(command.resolution_id, "cancel") {
            return Ok(absorbed);
        }
        self.guard_cancel()?;
        if let CancelRequester::Owner(caller) = &command.requester {
            self.guard_owner(caller)?;
        }
        let withdrawal = RunWithdrawal {
            reason_code: ReasonCode::new(CANCELLED_BY_JOB)?,
            requested_by: command.requester.known_user().cloned(),
            originating_job_id: command.requester.originating_job_id(),
        };
        let mut result = CommandResult::new(self.withdraw_active_run(&withdrawal));
        if self.active_run().is_some_and(Run::has_started) {
            result = result.with_warning(CommandWarning::CancellationIsBestEffort);
        }
        result.events.push(JobEvent::JobCancelled(JobCancelled {
            job_id: self.id(),
            resolution_id: command.resolution_id,
            originating_job_id: command.requester.originating_job_id(),
        }));
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::job::resolution::JobResolution;
    use crate::domain::keys::ProducerKey;
    use crate::domain::ownership::Caller;
    use crate::fixtures::{JobBuilder, RunBuilder, job_id, producer, resolution_id, ts, user};

    fn by_administrator() -> CancelRequester {
        CancelRequester::Administrator(user())
    }

    #[test]
    fn cancelling_queued_work_withdraws_its_undelivered_trigger() {
        // Given: a job whose first run was dispatched but never claimed
        let run = RunBuilder::new(1).build();
        let run_id = run.id();
        let job = JobBuilder::new().with_run(run).build();
        // When: an administrator cancels it
        let result = job
            .cancel(CancelJob {
                resolution_id: resolution_id(),
                requester: by_administrator(),
            })
            .unwrap();
        // Then: the queued run is cancelled outright, with no stop request needed
        match result.events.as_slice() {
            [JobEvent::RunCancelled(cancelled), JobEvent::JobCancelled(_)] => {
                assert_eq!(cancelled.run_id, run_id);
            }
            other => panic!("expected a run and a job cancellation, got {other:?}"),
        }
        assert!(result.warnings.is_empty());
    }

    #[test]
    fn cancelling_work_in_flight_records_a_stop_request_and_says_it_is_best_effort() {
        // Given: a job whose run an instance is executing
        let run = RunBuilder::new(1).started(ts(5)).build();
        let run_id = run.id();
        let job = JobBuilder::new().with_run(run).build();
        // When: an administrator cancels it
        let result = job
            .cancel(CancelJob {
                resolution_id: resolution_id(),
                requester: by_administrator(),
            })
            .unwrap();
        // Then: a stop request is recorded before the run and job are cancelled
        match result.events.as_slice() {
            [
                JobEvent::RunCancellationRequested(requested),
                JobEvent::RunCancelled(cancelled),
                JobEvent::JobCancelled(_),
            ] => {
                assert_eq!(requested.run_id, run_id);
                assert_eq!(requested.reason_code.as_str(), "job_cancelled");
                assert!(requested.requested_by.is_some());
                assert_eq!(cancelled.run_id, run_id);
            }
            other => panic!("expected a stop request and two cancellations, got {other:?}"),
        }
        assert_eq!(
            result.warnings,
            vec![CommandWarning::CancellationIsBestEffort]
        );
    }

    #[test]
    fn a_cascaded_cancellation_records_the_ancestor_that_caused_it() {
        // Given: a descendant job reached by a cancellation travelling downward
        let ancestor = job_id();
        let job = JobBuilder::new()
            .with_run(RunBuilder::new(1).build())
            .build();
        // When: the cascade cancels it
        let result = job
            .cancel(CancelJob {
                resolution_id: resolution_id(),
                requester: CancelRequester::Cascade {
                    originating_job_id: ancestor,
                },
            })
            .unwrap();
        // Then: each descendant carries its own cancellation, naming the origin
        match result.events.last() {
            Some(JobEvent::JobCancelled(fact)) => {
                assert_eq!(fact.job_id, job.id());
                assert_eq!(fact.originating_job_id, Some(ancestor));
            }
            other => panic!("expected a JobCancelled fact, got {other:?}"),
        }
    }

    #[test]
    fn a_pending_job_with_no_run_cancels_on_its_own() {
        // Given: a job that never got a run
        let job = JobBuilder::new().build();
        // When: it is cancelled
        let result = job
            .cancel(CancelJob {
                resolution_id: resolution_id(),
                requester: by_administrator(),
            })
            .unwrap();
        // Then: only the job resolution is recorded
        assert_eq!(result.events.len(), 1);
    }

    #[test]
    fn a_terminal_job_cannot_be_cancelled() {
        // Given: a job that already completed
        let job = JobBuilder::new()
            .with_resolution(JobResolution::completed(resolution_id(), ts(30)))
            .build();
        // When: a cancellation arrives
        let result = job.cancel(CancelJob {
            resolution_id: resolution_id(),
            requester: by_administrator(),
        });
        // Then: it is refused with the same code the affordance advertises
        assert_eq!(
            result,
            Err(JobsError::JobAlreadyTerminal {
                status: "COMPLETED"
            })
        );
    }

    #[test]
    fn a_redelivered_cancellation_is_absorbed() {
        // Given: a job already cancelled under a known resolution id
        let resolution = resolution_id();
        let job = JobBuilder::new()
            .with_resolution(JobResolution::cancelled(resolution, ts(30)))
            .build();
        // When: the same cancellation command is delivered again
        let result = job
            .cancel(CancelJob {
                resolution_id: resolution,
                requester: by_administrator(),
            })
            .unwrap();
        // Then: nothing is recorded twice
        assert!(result.is_empty());
    }

    #[test]
    fn an_owner_cancelling_a_job_it_does_not_own_is_refused() {
        // Given: a job owned by the projects bounded context
        let job = JobBuilder::new().build();
        // When: another producer cancels it as if it were the owner
        let result = job.cancel(CancelJob {
            resolution_id: resolution_id(),
            requester: CancelRequester::Owner(Caller::Producer(ProducerKey::new("chat").unwrap())),
        });
        // Then: ownership is checked before the cancellation is honoured
        assert_eq!(result, Err(JobsError::NotOwner));
    }

    #[test]
    fn the_rightful_owner_may_cancel_its_own_job() {
        // Given: a job owned by the producer that declared it
        let job = JobBuilder::new().build();
        // When: that producer cancels it
        let result = job.cancel(CancelJob {
            resolution_id: resolution_id(),
            requester: CancelRequester::Owner(Caller::Producer(producer())),
        });
        // Then: the cancellation is honoured
        assert!(result.is_ok());
    }
}
