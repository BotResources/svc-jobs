use crate::commands::{CommandResult, JobCommandResult};
use crate::domain::job::Job;
use crate::domain::references::KnownUser;
use crate::error::JobsError;
use crate::event::job::JobEvent;
use crate::event::job_facts::JobDeleted;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteJob {
    pub deleted_by: KnownUser,
}

impl Job {
    pub fn delete(&self, command: DeleteJob) -> Result<JobCommandResult, JobsError> {
        self.guard_delete()?;
        Ok(CommandResult::from_event(JobEvent::JobDeleted(
            JobDeleted {
                job_id: self.id(),
                deleted_by: command.deleted_by,
            },
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::job::resolution::JobResolution;
    use crate::fixtures::{JobBuilder, RunBuilder, job_id, resolution_id, ts, user};

    fn settled() -> JobBuilder {
        JobBuilder::new()
            .with_run(RunBuilder::new(1).started(ts(5)).completed(ts(20)).build())
            .with_resolution(JobResolution::completed(resolution_id(), ts(30)))
    }

    #[test]
    fn a_terminal_job_is_soft_deleted_by_the_administrator_that_asked() {
        // Given: a job that reached a terminal state
        let job = settled().build();
        // When: an administrator deletes it
        let result = job.delete(DeleteJob { deleted_by: user() }).unwrap();
        // Then: one deletion fact is recorded, naming who deleted it
        match result.events.as_slice() {
            [JobEvent::JobDeleted(fact)] => assert_eq!(fact.job_id, job.id()),
            other => panic!("expected a JobDeleted fact, got {other:?}"),
        }
    }

    #[test]
    fn live_work_is_never_deleted() {
        // Given: a job still in progress
        let job = JobBuilder::new()
            .with_run(RunBuilder::new(1).build())
            .build();
        // When: deletion is attempted
        let result = job.delete(DeleteJob { deleted_by: user() });
        // Then: it is refused with the status that blocks it
        assert_eq!(
            result,
            Err(JobsError::JobNotTerminal {
                status: "IN_PROGRESS"
            })
        );
    }

    #[test]
    fn a_predecessor_is_not_deleted_while_its_successor_still_runs() {
        // Given: a failed predecessor whose manual-retry successor is still live
        let successor = job_id();
        let job = settled().manually_retried_by(successor, false).build();
        // When: deletion is attempted
        let result = job.delete(DeleteJob { deleted_by: user() });
        // Then: the chain must settle first
        assert_eq!(
            result,
            Err(JobsError::SuccessorStillActive {
                successor_job_id: successor.as_uuid()
            })
        );
    }

    #[test]
    fn deleting_an_already_deleted_job_is_refused() {
        // Given: a job already soft-deleted
        let job = settled().deleted(ts(90)).build();
        // When: deletion is attempted again
        let result = job.delete(DeleteJob { deleted_by: user() });
        // Then: the second deletion is refused rather than duplicated
        assert_eq!(result, Err(JobsError::JobAlreadyDeleted));
    }
}
