use crate::commands::job::withdrawal::{CANCELLED_BY_JOB, RunWithdrawal};
use crate::commands::{CommandResult, CommandWarning, JobCommandResult};
use crate::domain::ids::{JobId, ResolutionId};
use crate::domain::job::Job;
use crate::domain::job::resolution::JobResolutionKind;
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
            Self::Owner(_) | Self::Declarant | Self::Cascade { .. } | Self::Caller { .. } => None,
        }
    }

    fn originating_job_id(&self) -> Option<JobId> {
        match self {
            Self::Cascade { originating_job_id } => Some(*originating_job_id),
            Self::Administrator(_) | Self::Owner(_) | Self::Declarant | Self::Caller { .. } => None,
        }
    }
}

impl Job {
    pub fn cancel(&self, command: CancelJob) -> Result<JobCommandResult, JobsError> {
        if let Some(absorbed) = self.already_resolved_by(
            command.resolution_id,
            JobResolutionKind::Cancelled,
            "cancel",
        )? {
            return Ok(absorbed);
        }
        if command.requester.may_cancel_a_terminal_job() && self.is_terminal() {
            command.requester.guard_may_cancel(self.requester())?;
            return Ok(CommandResult::nothing_happened(
                CommandWarning::CancelledJobAlreadyTerminal,
            ));
        }
        self.guard_cancel()?;
        command.requester.guard_may_cancel(self.requester())?;
        let withdrawal = RunWithdrawal {
            reason_code: ReasonCode::new(CANCELLED_BY_JOB)?,
            requested_by: command.requester.known_user().cloned(),
            originating_job_id: command.requester.originating_job_id(),
        };
        let mut result = CommandResult::new(self.withdraw_active_run(&withdrawal));
        if self.active_run().is_some_and(Run::is_held_by_an_instance) {
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
    use crate::domain::ids::RunId;
    use crate::domain::job::resolution::JobResolution;
    use crate::domain::ownership::{ActorRef, DeclarationClaim};
    use crate::domain::requester::{ActsFor, Caller, Requester};
    use crate::fixtures::{JobBuilder, RunBuilder, job_id, resolution_id, ts, user};
    use uuid::Uuid;

    fn by_administrator() -> CancelRequester {
        CancelRequester::Administrator(user())
    }

    #[test]
    fn a_cascade_reaching_a_terminal_job_changes_nothing_on_it() {
        let job = JobBuilder::new()
            .with_resolution(JobResolution::completed(resolution_id(), ts(30)))
            .build();
        let result = job
            .cancel(CancelJob {
                resolution_id: resolution_id(),
                requester: CancelRequester::Cascade {
                    originating_job_id: job_id(),
                },
            })
            .unwrap();
        assert!(result.events.is_empty());
        assert_eq!(
            result.warnings,
            vec![CommandWarning::CancelledJobAlreadyTerminal]
        );
    }

    #[test]
    fn cancelling_queued_work_withdraws_its_undelivered_trigger() {
        let run = RunBuilder::new(1).build();
        let run_id = run.id();
        let job = JobBuilder::new().with_run(run).build();
        let result = job
            .cancel(CancelJob {
                resolution_id: resolution_id(),
                requester: by_administrator(),
            })
            .unwrap();
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
        let run = RunBuilder::new(1).started(ts(5)).build();
        let run_id = run.id();
        let job = JobBuilder::new().with_run(run).build();
        let result = job
            .cancel(CancelJob {
                resolution_id: resolution_id(),
                requester: by_administrator(),
            })
            .unwrap();
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
        let ancestor = job_id();
        let job = JobBuilder::new()
            .with_run(RunBuilder::new(1).build())
            .build();
        let result = job
            .cancel(CancelJob {
                resolution_id: resolution_id(),
                requester: CancelRequester::Cascade {
                    originating_job_id: ancestor,
                },
            })
            .unwrap();
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
        let job = JobBuilder::new().build();
        let result = job
            .cancel(CancelJob {
                resolution_id: resolution_id(),
                requester: by_administrator(),
            })
            .unwrap();
        assert_eq!(result.events.len(), 1);
    }

    #[test]
    fn a_terminal_job_cannot_be_cancelled() {
        let job = JobBuilder::new()
            .with_resolution(JobResolution::completed(resolution_id(), ts(30)))
            .build();
        let result = job.cancel(CancelJob {
            resolution_id: resolution_id(),
            requester: by_administrator(),
        });
        assert_eq!(
            result,
            Err(JobsError::JobAlreadyTerminal {
                status: "COMPLETED"
            })
        );
    }

    #[test]
    fn a_redelivered_cancellation_is_absorbed() {
        let resolution = resolution_id();
        let job = JobBuilder::new()
            .with_resolution(JobResolution::cancelled(resolution, ts(30)))
            .build();
        let result = job
            .cancel(CancelJob {
                resolution_id: resolution,
                requester: by_administrator(),
            })
            .unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn a_cancellation_reusing_a_completion_resolution_id_is_rejected_not_acknowledged() {
        let resolution = resolution_id();
        let job = JobBuilder::new()
            .with_resolution(JobResolution::completed(resolution, ts(30)))
            .build();
        let result = job.cancel(CancelJob {
            resolution_id: resolution,
            requester: by_administrator(),
        });
        assert_eq!(
            result,
            Err(JobsError::JobAlreadyTerminal {
                status: "COMPLETED"
            })
        );
    }

    #[test]
    fn an_owner_cancelling_a_job_it_does_not_own_is_refused() {
        let job = JobBuilder::new().build();
        let result = job.cancel(CancelJob {
            resolution_id: resolution_id(),
            requester: CancelRequester::Owner(DeclarationClaim::new(
                Some(ActorRef::new(Uuid::now_v7())),
                ActorRef::new(Uuid::now_v7()),
            )),
        });
        assert_eq!(result, Err(JobsError::NotOwner));
    }

    #[test]
    fn the_rightful_owner_may_cancel_its_own_job() {
        let job = JobBuilder::new().build();
        let declared_by = ActorRef::new(Uuid::now_v7());
        let result = job.cancel(CancelJob {
            resolution_id: resolution_id(),
            requester: CancelRequester::Owner(DeclarationClaim::new(
                Some(declared_by),
                declared_by,
            )),
        });
        assert!(result.is_ok());
    }

    #[test]
    fn a_fabric_admitted_declarant_may_cancel_a_non_terminal_job() {
        let job = JobBuilder::new().build();
        let resolution = resolution_id();
        let result = job
            .cancel(CancelJob {
                resolution_id: resolution,
                requester: CancelRequester::Declarant,
            })
            .unwrap();
        match result.events.as_slice() {
            [JobEvent::JobCancelled(fact)] => {
                assert_eq!(fact.job_id, job.id());
                assert_eq!(fact.resolution_id, resolution);
                assert_eq!(fact.originating_job_id, None);
            }
            other => panic!("expected one JobCancelled fact, got {other:?}"),
        }
        assert!(result.warnings.is_empty());
    }

    #[test]
    fn a_fabric_admitted_declarant_cannot_cancel_a_terminal_job() {
        let job = JobBuilder::new()
            .with_resolution(JobResolution::completed(resolution_id(), ts(30)))
            .build();
        let result = job.cancel(CancelJob {
            resolution_id: resolution_id(),
            requester: CancelRequester::Declarant,
        });
        assert_eq!(
            result,
            Err(JobsError::JobAlreadyTerminal {
                status: "COMPLETED"
            })
        );
    }

    #[test]
    fn cancelling_a_run_reserved_by_a_claim_reaches_its_instance_through_a_stop_request() {
        let run = RunBuilder::new(1).reserved(ts(1)).build();
        let run_id = run.id();
        let job = JobBuilder::new().with_run(run).build();
        let result = job
            .cancel(CancelJob {
                resolution_id: resolution_id(),
                requester: by_administrator(),
            })
            .unwrap();
        match result.events.as_slice() {
            [
                JobEvent::RunCancellationRequested(requested),
                JobEvent::RunCancelled(cancelled),
                JobEvent::JobCancelled(_),
            ] => {
                assert_eq!(requested.run_id, run_id);
                assert_eq!(cancelled.run_id, run_id);
            }
            other => panic!("expected a stop request and two cancellations, got {other:?}"),
        }
        assert_eq!(
            result.warnings,
            vec![CommandWarning::CancellationIsBestEffort]
        );
    }

    fn service_caller(actor: ActorRef) -> CancelRequester {
        CancelRequester::Caller {
            caller: Caller::Service(actor),
            acts_for: ActsFor::default(),
        }
    }

    #[test]
    fn a_service_that_requested_the_job_may_cancel_it_and_a_terminal_job_cancels_as_a_no_op() {
        let actor = ActorRef::new(Uuid::from_u128(7));
        let job = JobBuilder::new()
            .with_requester(Requester::Service(Some(actor)))
            .with_resolution(JobResolution::completed(resolution_id(), ts(30)))
            .build();
        let result = job
            .cancel(CancelJob {
                resolution_id: resolution_id(),
                requester: service_caller(actor),
            })
            .unwrap();
        assert!(result.events.is_empty());
        assert_eq!(
            result.warnings,
            vec![CommandWarning::CancelledJobAlreadyTerminal]
        );
    }

    #[test]
    fn a_service_that_did_not_request_the_job_cannot_cancel_it() {
        let job = JobBuilder::new()
            .with_requester(Requester::Service(Some(ActorRef::new(Uuid::from_u128(7)))))
            .build();
        let result = job.cancel(CancelJob {
            resolution_id: resolution_id(),
            requester: service_caller(ActorRef::new(Uuid::from_u128(8))),
        });
        assert_eq!(result, Err(JobsError::NotRequester));
    }

    #[test]
    fn a_run_working_the_job_may_cancel_it_but_another_run_may_not() {
        let requesting_run = RunId::new(Uuid::now_v7()).unwrap();
        let job = JobBuilder::new()
            .with_requester(Requester::Run(requesting_run))
            .build();
        let by = |works_the_job| CancelRequester::Caller {
            caller: Caller::Run {
                run_id: RunId::new(Uuid::now_v7()).unwrap(),
                works_the_job,
            },
            acts_for: ActsFor::default(),
        };
        assert!(
            job.cancel(CancelJob {
                resolution_id: resolution_id(),
                requester: by(true)
            })
            .is_ok()
        );
        assert_eq!(
            job.cancel(CancelJob {
                resolution_id: resolution_id(),
                requester: by(false)
            }),
            Err(JobsError::RunnerNotWorkingTheJob)
        );
    }
}
