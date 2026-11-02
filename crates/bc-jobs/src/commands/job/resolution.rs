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
        command.requester.guard_may_fail(self.requester())?;
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
mod tests;
