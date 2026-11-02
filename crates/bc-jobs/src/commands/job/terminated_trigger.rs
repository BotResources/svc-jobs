use crate::commands::job::backstop::ReclaimRun;
use crate::commands::{CommandResult, CommandWarning, JobCommandResult};
use crate::domain::job::Job;
use crate::domain::policy::{RetryPolicy, ServiceLimits};
use crate::domain::run::failure::{RunFailureKind, RunFailureReport, TRIGGER_TERMINATED};
use crate::domain::run::start_standing::StartStanding;
use crate::error::JobsError;

impl Job {
    pub fn fail_run_for_terminated_trigger(
        &self,
        command: ReclaimRun,
        policy: &RetryPolicy,
        limits: &ServiceLimits,
    ) -> Result<JobCommandResult, JobsError> {
        let run = self.find_run(command.run_id)?;
        match run.start_standing() {
            StartStanding::Ended => Ok(Self::settled_discard("RunFailed", run)),
            StartStanding::HeldBy(_) => Ok(CommandResult::nothing_happened(
                CommandWarning::TriggerTerminationIgnored {
                    run_standing: "started",
                },
            )),
            StartStanding::ReservedFor(_) => Ok(CommandResult::nothing_happened(
                CommandWarning::TriggerTerminationIgnored {
                    run_standing: "reserved",
                },
            )),
            StartStanding::Pending => {
                let report =
                    RunFailureReport::platform(RunFailureKind::Permanent, TRIGGER_TERMINATED)?;
                let fact = command.into_failure(report);
                Ok(CommandResult::new(
                    self.run_failure_events(run, &fact, policy, limits)?,
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests;
