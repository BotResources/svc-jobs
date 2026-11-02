use crate::commands::{CommandResult, CommandWarning, JobCommandResult};
use crate::domain::ids::{RunId, StartId};
use crate::domain::job::Job;
use crate::domain::run::parts::RunnerInstanceReference;
use crate::domain::run::start_refusal::StartRefusal;
use crate::domain::run::start_standing::StartStanding;
use crate::event::job::JobEvent;
use crate::event::job_facts::RunStarted;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartRequestFact {
    pub run_id: RunId,
    pub instance: RunnerInstanceReference,
    pub start_id: StartId,
    pub instance_present: bool,
}

impl Job {
    pub fn record_run_started_request(
        &self,
        fact: StartRequestFact,
    ) -> Result<JobCommandResult, StartRefusal> {
        let run = self
            .find_run(fact.run_id)
            .map_err(|_| StartRefusal::RunUnknown)?;
        if fact.instance.runner_type() != self.runner_type() {
            return Err(StartRefusal::RunUnknown);
        }
        if self.is_terminal() {
            return Err(StartRefusal::RunNotPending);
        }
        match run.start_standing() {
            StartStanding::Ended => return Err(StartRefusal::RunNotPending),
            StartStanding::HeldBy(start) => {
                if start.instance() == &fact.instance && start.start_id() == Some(fact.start_id) {
                    return Ok(CommandResult::nothing_happened(
                        CommandWarning::FactAlreadyRecorded {
                            fact: "RunStartedRequest",
                        },
                    ));
                }
                return Err(StartRefusal::RunHeld);
            }
            StartStanding::ReservedFor(instance) if instance != &fact.instance => {
                return Err(StartRefusal::RunReservedElsewhere);
            }
            StartStanding::ReservedFor(_) | StartStanding::Pending => {}
        }
        if !fact.instance_present {
            return Err(StartRefusal::InstanceUnknown);
        }
        Ok(CommandResult::from_event(JobEvent::RunStarted(
            RunStarted {
                job_id: self.id(),
                run_id: fact.run_id,
                runner_type: self.runner_type().clone(),
                instance_key: fact.instance.instance_key().clone(),
                start_id: Some(fact.start_id),
            },
        )))
    }
}

#[cfg(test)]
mod tests;
