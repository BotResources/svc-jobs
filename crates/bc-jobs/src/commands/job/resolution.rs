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
        command.requester.guard_may_resolve(self.requester())?;
        self.resolve_asking_its_run_to_stop(JobEvent::JobCompleted(JobCompleted {
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
        command.requester.guard_may_resolve(self.requester())?;
        self.resolve_asking_its_run_to_stop(JobEvent::JobFailed(JobFailed {
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
        let job = JobBuilder::new()
            .with_run(RunBuilder::new(1).started(ts(5)).completed(ts(20)).build())
            .build();
        let resolution = resolution_id();
        let result = job
            .finish(FinishJob {
                resolution_id: resolution,
                requester: ResolutionRequester::LegacyOwner(owner()),
            })
            .unwrap();
        match result.events.as_slice() {
            [JobEvent::JobCompleted(fact)] => assert_eq!(fact.resolution_id, resolution),
            other => panic!("expected a JobCompleted fact, got {other:?}"),
        }
    }

    #[test]
    fn an_actor_that_did_not_declare_the_job_may_not_finish_it() {
        let job = JobBuilder::new().build();
        let result = job.finish(FinishJob {
            resolution_id: resolution_id(),
            requester: ResolutionRequester::LegacyOwner(DeclarationClaim::new(
                Some(declaring_actor()),
                ActorRef::new(Uuid::now_v7()),
            )),
        });
        assert_eq!(result, Err(JobsError::NotOwner));
    }

    #[test]
    fn a_child_job_is_finished_by_the_actor_that_declared_it() {
        let parent = job_id();
        let job = JobBuilder::new().with_parent(parent).build();
        let result = job.finish(FinishJob {
            resolution_id: resolution_id(),
            requester: ResolutionRequester::LegacyOwner(owner()),
        });
        assert!(result.is_ok());
    }

    #[test]
    fn an_owner_may_declare_the_job_failed_after_judging_a_report() {
        let job = JobBuilder::new().build();
        let result = job
            .declare_failed(FailJob {
                resolution_id: resolution_id(),
                requester: ResolutionRequester::LegacyOwner(owner()),
            })
            .unwrap();
        match result.events.as_slice() {
            [JobEvent::JobFailed(fact)] => {
                assert_eq!(fact.failure_cause, JobFailureCause::DeclaredByOwner);
                assert_eq!(fact.caused_by_run_id, None);
            }
            other => panic!("expected a JobFailed fact, got {other:?}"),
        }
    }

    #[test]
    fn finishing_a_job_asks_its_started_run_to_stop_and_leaves_it_to_end_by_its_own_fact() {
        let run = RunBuilder::new(1).started(ts(5)).build();
        let job = JobBuilder::new().with_run(run).build();
        let result = job
            .finish(FinishJob {
                resolution_id: resolution_id(),
                requester: ResolutionRequester::LegacyOwner(owner()),
            })
            .unwrap();
        assert!(matches!(
            result.events.as_slice(),
            [
                JobEvent::RunCancellationRequested(_),
                JobEvent::JobCompleted(_)
            ]
        ));
        assert_eq!(
            result.warnings,
            vec![CommandWarning::CancellationIsBestEffort]
        );
    }

    #[test]
    fn declaring_a_job_failed_asks_its_started_run_to_stop_and_leaves_it_to_end_by_its_own_fact() {
        let run = RunBuilder::new(1).started(ts(5)).build();
        let job = JobBuilder::new().with_run(run).build();
        let result = job
            .declare_failed(FailJob {
                resolution_id: resolution_id(),
                requester: ResolutionRequester::LegacyOwner(owner()),
            })
            .unwrap();
        assert!(matches!(
            result.events.as_slice(),
            [
                JobEvent::RunCancellationRequested(_),
                JobEvent::JobFailed(_)
            ]
        ));
    }

    #[test]
    fn failing_a_job_withdraws_the_trigger_of_a_run_no_instance_ever_claimed() {
        let run = RunBuilder::new(1).build();
        let run_id = run.id();
        let job = JobBuilder::new().with_run(run).build();
        let result = job
            .declare_failed(FailJob {
                resolution_id: resolution_id(),
                requester: ResolutionRequester::LegacyOwner(owner()),
            })
            .unwrap();
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
        let job = JobBuilder::new()
            .with_resolution(JobResolution::completed(resolution_id(), ts(30)))
            .build();
        let result = job.declare_failed(FailJob {
            resolution_id: resolution_id(),
            requester: ResolutionRequester::LegacyOwner(owner()),
        });
        assert_eq!(
            result,
            Err(JobsError::JobAlreadyTerminal {
                status: "COMPLETED"
            })
        );
    }

    #[test]
    fn a_redelivered_resolution_command_is_absorbed() {
        let resolution = resolution_id();
        let job = JobBuilder::new()
            .with_resolution(JobResolution::completed(resolution, ts(30)))
            .build();
        let result = job
            .finish(FinishJob {
                resolution_id: resolution,
                requester: ResolutionRequester::LegacyOwner(owner()),
            })
            .unwrap();
        assert!(result.is_empty());
        assert_eq!(
            result.warnings,
            vec![CommandWarning::CommandAlreadyApplied { command: "finish" }]
        );
    }

    #[test]
    fn a_deleted_job_accepts_no_resolution() {
        let job = JobBuilder::new()
            .with_resolution(JobResolution::completed(resolution_id(), ts(30)))
            .deleted(ts(90))
            .build();
        let result = job.finish(FinishJob {
            resolution_id: resolution_id(),
            requester: ResolutionRequester::LegacyOwner(owner()),
        });
        assert_eq!(result, Err(JobsError::JobDeleted));
    }

    #[test]
    fn a_fabric_admitted_declarant_may_finish_a_non_terminal_job() {
        let run = RunBuilder::new(1).started(ts(5)).build();
        let job = JobBuilder::new().with_run(run).build();
        let resolution = resolution_id();
        let result = job
            .finish(FinishJob {
                resolution_id: resolution,
                requester: ResolutionRequester::Declarant,
            })
            .unwrap();
        match result.events.as_slice() {
            [
                JobEvent::RunCancellationRequested(_),
                JobEvent::JobCompleted(fact),
            ] => {
                assert_eq!(fact.job_id, job.id());
                assert_eq!(fact.resolution_id, resolution);
            }
            other => panic!("expected a stop request and JobCompleted, got {other:?}"),
        }
    }

    #[test]
    fn a_fabric_admitted_declarant_may_fail_a_non_terminal_job() {
        let run = RunBuilder::new(1).build();
        let run_id = run.id();
        let job = JobBuilder::new().with_run(run).build();
        let resolution = resolution_id();
        let result = job
            .declare_failed(FailJob {
                resolution_id: resolution,
                requester: ResolutionRequester::Declarant,
            })
            .unwrap();
        match result.events.as_slice() {
            [JobEvent::RunCancelled(cancelled), JobEvent::JobFailed(fact)] => {
                assert_eq!(cancelled.run_id, run_id);
                assert_eq!(fact.job_id, job.id());
                assert_eq!(fact.resolution_id, resolution);
                assert_eq!(fact.failure_cause, JobFailureCause::DeclaredByOwner);
                assert_eq!(fact.caused_by_run_id, None);
            }
            other => panic!("expected trigger withdrawal followed by JobFailed, got {other:?}"),
        }
        assert!(result.warnings.is_empty());
    }

    #[test]
    fn a_fabric_admitted_declarant_cannot_replace_a_terminal_resolution() {
        let job = JobBuilder::new()
            .with_resolution(JobResolution::completed(resolution_id(), ts(30)))
            .build();
        let result = job.declare_failed(FailJob {
            resolution_id: resolution_id(),
            requester: ResolutionRequester::Declarant,
        });
        assert_eq!(
            result,
            Err(JobsError::JobAlreadyTerminal {
                status: "COMPLETED"
            })
        );
    }

    #[test]
    fn a_fabric_admitted_declarant_cannot_resolve_a_deleted_job() {
        let job = JobBuilder::new()
            .with_resolution(JobResolution::completed(resolution_id(), ts(30)))
            .deleted(ts(90))
            .build();
        let result = job.finish(FinishJob {
            resolution_id: resolution_id(),
            requester: ResolutionRequester::Declarant,
        });
        assert_eq!(result, Err(JobsError::JobDeleted));
    }
}
