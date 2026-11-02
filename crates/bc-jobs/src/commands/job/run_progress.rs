use chrono::{DateTime, Utc};

use crate::commands::{CommandResult, CommandWarning, JobCommandResult};
use crate::domain::ids::{PlanDeclarationId, RunId};
use crate::domain::job::Job;
use crate::domain::keys::StepLabel;
use crate::domain::run::Run;
use crate::domain::run::parts::RunnerInstanceReference;
use crate::domain::run::plan::{DeclarationNumber, RunPlan, RunPlanItem};
use crate::domain::run::start_standing::StartStanding;
use crate::domain::run::step::{Step, StepIndex};
use crate::error::JobsError;
use crate::event::job::JobEvent;
use crate::event::job_facts::{RunPlanDeclared, RunStarted, RunStepStarted};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunStartedFact {
    pub run_id: RunId,
    pub instance: RunnerInstanceReference,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunPlanFact {
    pub run_id: RunId,
    pub declaration_id: PlanDeclarationId,
    pub items: Vec<RunPlanItem>,
    pub declared_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunStepFact {
    pub run_id: RunId,
    pub step_index: StepIndex,
    pub label: StepLabel,
    pub started_at: DateTime<Utc>,
}

impl Job {
    pub(crate) fn discard_settled(
        &self,
        fact: &'static str,
        run: &Run,
    ) -> Option<JobCommandResult> {
        matches!(run.start_standing(), StartStanding::Ended)
            .then(|| Self::settled_discard(fact, run))
    }

    pub(crate) fn settled_discard(fact: &'static str, run: &Run) -> JobCommandResult {
        CommandResult::nothing_happened(CommandWarning::FactDiscardedOnTerminalRun {
            fact,
            run_status: run.status().as_db_str(),
        })
    }

    pub(crate) fn discard_on_terminal_job(&self, fact: &'static str) -> Option<JobCommandResult> {
        self.is_terminal().then(|| {
            CommandResult::nothing_happened(CommandWarning::FactDiscardedOnTerminalJob {
                fact,
                job_status: self.status().as_db_str(),
            })
        })
    }

    pub fn record_run_started(&self, fact: RunStartedFact) -> Result<JobCommandResult, JobsError> {
        if let Some(discarded) = self.discard_on_terminal_job("RunStarted") {
            return Ok(discarded);
        }
        let run = self.find_run(fact.run_id)?;
        let standing = run.start_standing();
        if matches!(standing, StartStanding::Ended) {
            return Ok(Self::settled_discard("RunStarted", run));
        }
        if fact.instance.runner_type() != self.runner_type() {
            return Err(JobsError::RunnerTypeMismatch {
                expected: self.runner_type().as_str().to_owned(),
                claimed: fact.instance.runner_type().as_str().to_owned(),
            });
        }
        match standing {
            StartStanding::HeldBy(_) => {
                return Ok(CommandResult::nothing_happened(
                    CommandWarning::FactAlreadyRecorded { fact: "RunStarted" },
                ));
            }
            StartStanding::ReservedFor(instance) if instance != &fact.instance => {
                return Ok(CommandResult::nothing_happened(
                    CommandWarning::FactDiscardedReservedElsewhere { fact: "RunStarted" },
                ));
            }
            StartStanding::Ended | StartStanding::Pending | StartStanding::ReservedFor(_) => {}
        }
        Ok(CommandResult::from_event(JobEvent::RunStarted(
            RunStarted {
                job_id: self.id(),
                run_id: fact.run_id,
                runner_type: self.runner_type().clone(),
                instance_key: fact.instance.instance_key().clone(),
                start_id: None,
            },
        )))
    }

    pub fn declare_run_plan(&self, fact: RunPlanFact) -> Result<JobCommandResult, JobsError> {
        if let Some(discarded) = self.discard_on_terminal_job("RunPlanDeclared") {
            return Ok(discarded);
        }
        let run = self.find_run(fact.run_id)?;
        if let Some(discarded) = self.discard_settled("RunPlanDeclared", run) {
            return Ok(discarded);
        }
        if run
            .plan()
            .is_some_and(|current| current.items() == fact.items)
        {
            return Ok(CommandResult::nothing_happened(
                CommandWarning::FactAlreadyRecorded {
                    fact: "RunPlanDeclared",
                },
            ));
        }
        let declaration_number = run.plan().map_or(DeclarationNumber::FIRST, |current| {
            current.declaration_number().next()
        });
        let plan = RunPlan::new(
            fact.declaration_id,
            declaration_number,
            fact.declared_at,
            fact.items,
        )?;
        Ok(CommandResult::from_event(JobEvent::RunPlanDeclared(
            RunPlanDeclared {
                job_id: self.id(),
                run_id: fact.run_id,
                declaration_id: plan.declaration_id(),
                declaration_number: plan.declaration_number(),
                items: plan.items().to_vec(),
            },
        )))
    }

    pub fn record_step_started(&self, fact: RunStepFact) -> Result<JobCommandResult, JobsError> {
        if let Some(discarded) = self.discard_on_terminal_job("RunStepStarted") {
            return Ok(discarded);
        }
        let run = self.find_run(fact.run_id)?;
        if let Some(discarded) = self.discard_settled("RunStepStarted", run) {
            return Ok(discarded);
        }
        let current = run.current_step().map(Step::index);
        if current.is_some_and(|current| current >= fact.step_index) {
            return Ok(CommandResult::nothing_happened(
                CommandWarning::StepIndexNotAdvancing {
                    current: current.unwrap_or(StepIndex::FIRST).get(),
                    submitted: fact.step_index.get(),
                },
            ));
        }
        Ok(CommandResult::from_event(JobEvent::RunStepStarted(
            RunStepStarted {
                job_id: self.id(),
                run_id: fact.run_id,
                step_index: fact.step_index,
                label: fact.label,
                started_at: fact.started_at,
                closed_step_index: current,
            },
        )))
    }
}

#[cfg(test)]
mod started_tests;
#[cfg(test)]
mod tests;
