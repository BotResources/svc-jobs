use crate::domain::ids::CommandId;
use crate::domain::job::Job;
use crate::domain::job::resolution::{JobResolution, JobResolutionKind};
use crate::domain::ownership::{CancelRequester, ResolutionRequester};
use crate::error::JobsError;
use crate::event::job::JobEvent;
use crate::event::job_facts::{JobCancelled, JobCompleted, JobFailed};

impl Job {
    fn resolved_by_command(
        &self,
        command_id: CommandId,
        kind: JobResolutionKind,
    ) -> Option<&JobResolution> {
        self.resolution().filter(|resolution| {
            resolution.kind() == kind && resolution.command_id() == Some(command_id)
        })
    }

    pub fn repeat_of_finish(
        &self,
        command_id: CommandId,
        requester: &ResolutionRequester,
    ) -> Result<Option<JobEvent>, JobsError> {
        let Some(resolution) = self.resolved_by_command(command_id, JobResolutionKind::Completed)
        else {
            return Ok(None);
        };
        requester.guard_may_resolve(self.requester())?;
        Ok(Some(JobEvent::JobCompleted(JobCompleted {
            job_id: self.id(),
            resolution_id: resolution.id(),
        })))
    }

    pub fn repeat_of_fail(
        &self,
        command_id: CommandId,
        requester: &ResolutionRequester,
    ) -> Result<Option<JobEvent>, JobsError> {
        let Some(resolution) = self.resolved_by_command(command_id, JobResolutionKind::Failed)
        else {
            return Ok(None);
        };
        requester.guard_may_resolve(self.requester())?;
        let failure_cause = resolution.failure_cause().ok_or(JobsError::CorruptState {
            reason_code: "failed_job_without_cause",
        })?;
        Ok(Some(JobEvent::JobFailed(JobFailed {
            job_id: self.id(),
            resolution_id: resolution.id(),
            failure_cause,
            caused_by_run_id: resolution.caused_by_run_id(),
            report: None,
        })))
    }

    pub fn repeat_of_cancel(
        &self,
        command_id: CommandId,
        requester: &CancelRequester,
    ) -> Result<Option<JobEvent>, JobsError> {
        let Some(resolution) = self.resolved_by_command(command_id, JobResolutionKind::Cancelled)
        else {
            return Ok(None);
        };
        requester.guard_may_cancel(self.requester())?;
        Ok(Some(JobEvent::JobCancelled(JobCancelled {
            job_id: self.id(),
            resolution_id: resolution.id(),
            originating_job_id: None,
        })))
    }
}

#[cfg(test)]
mod tests;
