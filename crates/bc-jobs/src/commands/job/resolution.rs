use crate::commands::{CommandResult, CommandWarning, JobCommandResult};
use crate::domain::ids::ResolutionId;
use crate::domain::job::Job;
use crate::domain::job::resolution::JobFailureCause;
use crate::domain::ownership::Caller;
use crate::error::JobsError;
use crate::event::job::JobEvent;
use crate::event::job_facts::{JobCompleted, JobFailed};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinishJob {
    pub resolution_id: ResolutionId,
    pub caller: Caller,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailJob {
    pub resolution_id: ResolutionId,
    pub caller: Caller,
}

impl Job {
    pub(crate) fn already_resolved_by(
        &self,
        resolution_id: ResolutionId,
        command: &'static str,
    ) -> Option<JobCommandResult> {
        self.resolution()
            .filter(|resolution| resolution.id() == resolution_id)
            .map(|_| {
                CommandResult::nothing_happened(CommandWarning::CommandAlreadyApplied { command })
            })
    }

    pub fn finish(&self, command: FinishJob) -> Result<JobCommandResult, JobsError> {
        if let Some(absorbed) = self.already_resolved_by(command.resolution_id, "finish") {
            return Ok(absorbed);
        }
        self.guard_not_deleted()?;
        self.guard_not_terminal()?;
        self.guard_owner(&command.caller)?;
        Ok(CommandResult::from_event(JobEvent::JobCompleted(
            JobCompleted {
                job_id: self.id(),
                resolution_id: command.resolution_id,
            },
        )))
    }

    pub fn declare_failed(&self, command: FailJob) -> Result<JobCommandResult, JobsError> {
        if let Some(absorbed) = self.already_resolved_by(command.resolution_id, "fail") {
            return Ok(absorbed);
        }
        self.guard_not_deleted()?;
        self.guard_not_terminal()?;
        self.guard_owner(&command.caller)?;
        Ok(CommandResult::from_event(JobEvent::JobFailed(JobFailed {
            job_id: self.id(),
            resolution_id: command.resolution_id,
            failure_cause: JobFailureCause::DeclaredByOwner,
            caused_by_run_id: None,
            report: None,
        })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::job::resolution::JobResolution;
    use crate::domain::keys::ProducerKey;
    use crate::fixtures::{
        JobBuilder, RunBuilder, job_id, producer, resolution_id, runner_type, ts,
    };

    fn owner() -> Caller {
        Caller::Producer(producer())
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
                caller: owner(),
            })
            .unwrap();
        // Then: exactly one completion fact is recorded, under the owner's resolution id
        match result.events.as_slice() {
            [JobEvent::JobCompleted(fact)] => assert_eq!(fact.resolution_id, resolution),
            other => panic!("expected a JobCompleted fact, got {other:?}"),
        }
    }

    #[test]
    fn a_caller_that_is_not_the_owner_may_not_finish_the_job() {
        // Given: a job owned by the projects bounded context
        let job = JobBuilder::new().build();
        // When: another producer claims it finished
        let result = job.finish(FinishJob {
            resolution_id: resolution_id(),
            caller: Caller::Producer(ProducerKey::new("chat").unwrap()),
        });
        // Then: only the owner finishes a job
        assert_eq!(result, Err(JobsError::NotOwner));
    }

    #[test]
    fn a_child_job_is_finished_by_the_runner_executing_its_parent() {
        // Given: a child job whose owner is the runner of the parent
        let parent = job_id();
        let job = JobBuilder::new().with_parent(parent).build();
        // When: that runner declares the child finished
        let result = job.finish(FinishJob {
            resolution_id: resolution_id(),
            caller: Caller::Runner {
                runner_type: runner_type(),
                executing_job_id: parent,
            },
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
                caller: owner(),
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
    fn a_terminal_job_is_never_resolved_a_second_time() {
        // Given: a job already completed
        let job = JobBuilder::new()
            .with_resolution(JobResolution::completed(resolution_id(), ts(30)))
            .build();
        // When: the owner sends a fresh failure declaration
        let result = job.declare_failed(FailJob {
            resolution_id: resolution_id(),
            caller: owner(),
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
                caller: owner(),
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
            caller: owner(),
        });
        // Then: the audit record stays as it was
        assert_eq!(result, Err(JobsError::JobDeleted));
    }
}
