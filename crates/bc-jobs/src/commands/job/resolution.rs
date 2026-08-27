use crate::commands::{CommandResult, CommandWarning, JobCommandResult};
use crate::domain::ids::ResolutionId;
use crate::domain::job::Job;
use crate::domain::job::resolution::{JobFailureCause, JobResolutionKind};
use crate::domain::ownership::ResolutionRequester;
use crate::error::JobsError;
use crate::event::job::JobEvent;
use crate::event::job_facts::{JobCompleted, JobFailed};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinishJob {
    pub resolution_id: ResolutionId,
    pub requester: ResolutionRequester,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailJob {
    pub resolution_id: ResolutionId,
    pub requester: ResolutionRequester,
}

impl Job {
    pub(crate) fn already_resolved_by(
        &self,
        resolution_id: ResolutionId,
        kind: JobResolutionKind,
        command: &'static str,
    ) -> Result<Option<JobCommandResult>, JobsError> {
        let Some(resolution) = self
            .resolution()
            .filter(|resolution| resolution.id() == resolution_id)
        else {
            return Ok(None);
        };
        if resolution.kind() != kind {
            return Err(JobsError::JobAlreadyTerminal {
                status: self.status().as_db_str(),
            });
        }
        Ok(Some(CommandResult::nothing_happened(
            CommandWarning::CommandAlreadyApplied { command },
        )))
    }

    pub fn finish(&self, command: FinishJob) -> Result<JobCommandResult, JobsError> {
        if let Some(absorbed) = self.already_resolved_by(
            command.resolution_id,
            JobResolutionKind::Completed,
            "finish",
        )? {
            return Ok(absorbed);
        }
        self.guard_not_deleted()?;
        self.guard_not_terminal()?;
        command.requester.guard_may_resolve()?;
        self.resolve_after_withdrawing_its_run(JobEvent::JobCompleted(JobCompleted {
            job_id: self.id(),
            resolution_id: command.resolution_id,
        }))
    }

    pub fn declare_failed(&self, command: FailJob) -> Result<JobCommandResult, JobsError> {
        if let Some(absorbed) =
            self.already_resolved_by(command.resolution_id, JobResolutionKind::Failed, "fail")?
        {
            return Ok(absorbed);
        }
        self.guard_not_deleted()?;
        self.guard_not_terminal()?;
        command.requester.guard_may_resolve()?;
        self.resolve_after_withdrawing_its_run(JobEvent::JobFailed(JobFailed {
            job_id: self.id(),
            resolution_id: command.resolution_id,
            failure_cause: JobFailureCause::DeclaredByOwner,
            caused_by_run_id: None,
            report: None,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::job::resolution::JobResolution;
    use crate::domain::ownership::{ActorRef, DeclarationClaim};
    use crate::fixtures::{JobBuilder, RunBuilder, job_id, resolution_id, ts};
    use uuid::Uuid;

    fn declaring_actor() -> ActorRef {
        ActorRef::new(Uuid::from_u128(0x019f_8137_e784_7320_87f3_1307_4aac_c4d4))
    }

    fn owner() -> DeclarationClaim {
        DeclarationClaim::new(Some(declaring_actor()), declaring_actor())
    }

    #[test]
    fn the_owner_finishing_a_job_is_the_only_path_to_completed() {
        // Given: a job in progress whose runs are irrelevant to its completion
        let job = JobBuilder::new()
            .with_run(RunBuilder::new(1).started(ts(5)).completed(ts(20)).build())
            .build();
        let resolution = resolution_id();
        // When: its owner declares it finished
        let result = job
            .finish(FinishJob {
                resolution_id: resolution,
                requester: ResolutionRequester::LegacyOwner(owner()),
            })
            .unwrap();
        // Then: exactly one completion fact is recorded, under the owner's resolution id
        match result.events.as_slice() {
            [JobEvent::JobCompleted(fact)] => assert_eq!(fact.resolution_id, resolution),
            other => panic!("expected a JobCompleted fact, got {other:?}"),
        }
    }

    #[test]
    fn an_actor_that_did_not_declare_the_job_may_not_finish_it() {
        // Given: a job declared by one actor
        let job = JobBuilder::new().build();
        // When: another actor claims it finished
        let result = job.finish(FinishJob {
            resolution_id: resolution_id(),
            requester: ResolutionRequester::LegacyOwner(DeclarationClaim::new(
                Some(declaring_actor()),
                ActorRef::new(Uuid::now_v7()),
            )),
        });
        // Then: only the actor that declared the job resolves it
        assert_eq!(result, Err(JobsError::NotOwner));
    }

    #[test]
    fn a_child_job_is_finished_by_the_actor_that_declared_it() {
        // Given: a child job declared by the runner executing its parent
        let parent = job_id();
        let job = JobBuilder::new().with_parent(parent).build();
        // When: that same actor declares the child finished
        let result = job.finish(FinishJob {
            resolution_id: resolution_id(),
            requester: ResolutionRequester::LegacyOwner(owner()),
        });
        // Then: it is accepted
        assert!(result.is_ok());
    }

    #[test]
    fn an_owner_may_declare_the_job_failed_after_judging_a_report() {
        // Given: a job whose child reported an unrecoverable failure
        let job = JobBuilder::new().build();
        // When: its owner declares it failed
        let result = job
            .declare_failed(FailJob {
                resolution_id: resolution_id(),
                requester: ResolutionRequester::LegacyOwner(owner()),
            })
            .unwrap();
        // Then: the recorded cause is the owner's declaration, with no run to blame
        match result.events.as_slice() {
            [JobEvent::JobFailed(fact)] => {
                assert_eq!(fact.failure_cause, JobFailureCause::DeclaredByOwner);
                assert_eq!(fact.caused_by_run_id, None);
            }
            other => panic!("expected a JobFailed fact, got {other:?}"),
        }
    }

    #[test]
    fn finishing_a_job_whose_run_is_still_executing_stops_that_run_first() {
        // Given: a job the owner considers done while an instance still executes its run
        let run = RunBuilder::new(1).started(ts(5)).build();
        let run_id = run.id();
        let job = JobBuilder::new().with_run(run).build();
        // When: the owner declares it finished
        let result = job
            .finish(FinishJob {
                resolution_id: resolution_id(),
                requester: ResolutionRequester::LegacyOwner(owner()),
            })
            .unwrap();
        // Then: the runner is told to stop and the run is closed before the job resolves
        match result.events.as_slice() {
            [
                JobEvent::RunCancellationRequested(requested),
                JobEvent::RunCancelled(cancelled),
                JobEvent::JobCompleted(_),
            ] => {
                assert_eq!(requested.run_id, run_id);
                assert_eq!(requested.reason_code.as_str(), "job_resolved");
                assert_eq!(cancelled.run_id, run_id);
            }
            other => {
                panic!("expected a stop request, a run closure and a completion, got {other:?}")
            }
        }
        assert_eq!(
            result.warnings,
            vec![CommandWarning::CancellationIsBestEffort]
        );
    }

    #[test]
    fn failing_a_job_withdraws_the_trigger_of_a_run_no_instance_ever_claimed() {
        // Given: a job whose only run was dispatched but never started
        let run = RunBuilder::new(1).build();
        let run_id = run.id();
        let job = JobBuilder::new().with_run(run).build();
        // When: the owner declares the job failed
        let result = job
            .declare_failed(FailJob {
                resolution_id: resolution_id(),
                requester: ResolutionRequester::LegacyOwner(owner()),
            })
            .unwrap();
        // Then: the queued run is closed outright, with nothing to stop
        match result.events.as_slice() {
            [JobEvent::RunCancelled(cancelled), JobEvent::JobFailed(_)] => {
                assert_eq!(cancelled.run_id, run_id);
            }
            other => panic!("expected a run closure and a failure, got {other:?}"),
        }
        assert!(result.warnings.is_empty());
    }

    #[test]
    fn a_terminal_job_is_never_resolved_a_second_time() {
        // Given: a job already completed
        let job = JobBuilder::new()
            .with_resolution(JobResolution::completed(resolution_id(), ts(30)))
            .build();
        // When: the owner sends a fresh failure declaration
        let result = job.declare_failed(FailJob {
            resolution_id: resolution_id(),
            requester: ResolutionRequester::LegacyOwner(owner()),
        });
        // Then: terminal resolutions are immutable
        assert_eq!(
            result,
            Err(JobsError::JobAlreadyTerminal {
                status: "COMPLETED"
            })
        );
    }

    #[test]
    fn a_redelivered_resolution_command_is_absorbed() {
        // Given: a job completed under a known resolution id
        let resolution = resolution_id();
        let job = JobBuilder::new()
            .with_resolution(JobResolution::completed(resolution, ts(30)))
            .build();
        // When: the same command is delivered again
        let result = job
            .finish(FinishJob {
                resolution_id: resolution,
                requester: ResolutionRequester::LegacyOwner(owner()),
            })
            .unwrap();
        // Then: no second resolution and no duplicate history
        assert!(result.is_empty());
        assert_eq!(
            result.warnings,
            vec![CommandWarning::CommandAlreadyApplied { command: "finish" }]
        );
    }

    #[test]
    fn a_deleted_job_accepts_no_resolution() {
        // Given: a soft-deleted job
        let job = JobBuilder::new()
            .with_resolution(JobResolution::completed(resolution_id(), ts(30)))
            .deleted(ts(90))
            .build();
        // When: a new resolution is attempted
        let result = job.finish(FinishJob {
            resolution_id: resolution_id(),
            requester: ResolutionRequester::LegacyOwner(owner()),
        });
        // Then: the audit record stays as it was
        assert_eq!(result, Err(JobsError::JobDeleted));
    }

    #[test]
    fn a_fabric_admitted_declarant_may_finish_a_non_terminal_job() {
        // Given: a non-terminal job and a v2 declaration admitted by the fabric
        let job = JobBuilder::new().build();
        // When: Jobs evaluates the lifecycle without interpreting actor metadata as authority
        let result = job.finish(FinishJob {
            resolution_id: resolution_id(),
            requester: ResolutionRequester::Declarant,
        });
        // Then: the lifecycle transition is admitted
        assert!(result.is_ok());
    }

    #[test]
    fn a_fabric_admitted_declarant_may_fail_a_non_terminal_job() {
        // Given: a non-terminal job and a v2 declaration admitted by the fabric
        let job = JobBuilder::new().build();
        // When: the declarant reports that the job failed
        let result = job.declare_failed(FailJob {
            resolution_id: resolution_id(),
            requester: ResolutionRequester::Declarant,
        });
        // Then: the lifecycle transition is admitted
        assert!(result.is_ok());
    }
}
