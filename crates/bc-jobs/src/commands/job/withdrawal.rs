use crate::commands::{CommandWarning, JobCommandResult};
use crate::domain::ids::JobId;
use crate::domain::job::Job;
use crate::domain::keys::ReasonCode;
use crate::domain::references::KnownUser;
use crate::domain::run::Run;
use crate::error::JobsError;
use crate::event::job::JobEvent;
use crate::event::job_facts::{RunCancellationRequested, RunCancelled};

pub const CANCELLED_BY_JOB: &str = "job_cancelled";
pub const RESOLVED_WITHOUT_RUN: &str = "job_resolved";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunWithdrawal {
    pub reason_code: ReasonCode,
    pub requested_by: Option<KnownUser>,
    pub originating_job_id: Option<JobId>,
}

impl RunWithdrawal {
    pub fn for_resolution() -> Result<Self, JobsError> {
        Ok(Self {
            reason_code: ReasonCode::new(RESOLVED_WITHOUT_RUN)?,
            requested_by: None,
            originating_job_id: None,
        })
    }
}

impl Job {
    pub(crate) fn withdraw_active_run(&self, withdrawal: &RunWithdrawal) -> Vec<JobEvent> {
        let Some(run) = self.active_run() else {
            return vec![];
        };
        let mut events = Vec::with_capacity(2);
        if run.is_held_by_an_instance() {
            events.push(self.stop_request(run, withdrawal));
        }
        events.push(JobEvent::RunCancelled(RunCancelled {
            job_id: self.id(),
            run_id: run.id(),
            withdrawn_for: None,
        }));
        events
    }

    pub(crate) fn resolve_asking_its_run_to_stop(
        &self,
        resolution: JobEvent,
    ) -> Result<JobCommandResult, JobsError> {
        let withdrawal = RunWithdrawal::for_resolution()?;
        let mut result = match self.active_run() {
            Some(run) if run.is_held_by_an_instance() => {
                JobCommandResult::new(vec![self.stop_request(run, &withdrawal)])
                    .with_warning(CommandWarning::CancellationIsBestEffort)
            }
            Some(_) => JobCommandResult::new(self.withdraw_active_run(&withdrawal)),
            None => JobCommandResult::new(vec![]),
        };
        result.events.push(resolution);
        Ok(result)
    }

    fn stop_request(&self, run: &Run, withdrawal: &RunWithdrawal) -> JobEvent {
        JobEvent::RunCancellationRequested(RunCancellationRequested {
            job_id: self.id(),
            run_id: run.id(),
            reason_code: withdrawal.reason_code.clone(),
            requested_by: withdrawal.requested_by.clone(),
            originating_job_id: withdrawal.originating_job_id,
        })
    }
}
