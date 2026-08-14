pub mod fleet;
pub mod job;
pub mod log;

use serde_json::{Value, json};

use crate::event::fleet::FleetEvent;
use crate::event::job::JobEvent;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandResult<E> {
    pub events: Vec<E>,
    pub warnings: Vec<CommandWarning>,
}

pub type JobCommandResult = CommandResult<JobEvent>;
pub type FleetCommandResult = CommandResult<FleetEvent>;

impl<E> CommandResult<E> {
    pub fn new(events: Vec<E>) -> Self {
        Self {
            events,
            warnings: vec![],
        }
    }

    pub fn from_event(event: E) -> Self {
        Self::new(vec![event])
    }

    pub fn nothing_happened(warning: CommandWarning) -> Self {
        Self {
            events: vec![],
            warnings: vec![warning],
        }
    }

    pub fn with_warning(mut self, warning: CommandWarning) -> Self {
        self.warnings.push(warning);
        self
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandWarning {
    FactDiscardedOnTerminalJob {
        fact: &'static str,
        job_status: &'static str,
    },
    FactDiscardedOnTerminalRun {
        fact: &'static str,
        run_status: &'static str,
    },
    FactAlreadyRecorded {
        fact: &'static str,
    },
    StepIndexNotAdvancing {
        current: u32,
        submitted: u32,
    },
    CancellationIsBestEffort,
    CommandAlreadyApplied {
        command: &'static str,
    },
}

impl CommandWarning {
    pub fn code(&self) -> &'static str {
        match self {
            Self::FactDiscardedOnTerminalJob { .. } => "fact_discarded_on_terminal_job",
            Self::FactDiscardedOnTerminalRun { .. } => "fact_discarded_on_terminal_run",
            Self::FactAlreadyRecorded { .. } => "fact_already_recorded",
            Self::StepIndexNotAdvancing { .. } => "step_index_not_advancing",
            Self::CancellationIsBestEffort => "cancellation_is_best_effort",
            Self::CommandAlreadyApplied { .. } => "command_already_applied",
        }
    }

    pub fn params(&self) -> Value {
        match self {
            Self::FactDiscardedOnTerminalJob { fact, job_status } => {
                json!({ "fact": fact, "jobStatus": job_status })
            }
            Self::FactDiscardedOnTerminalRun { fact, run_status } => {
                json!({ "fact": fact, "runStatus": run_status })
            }
            Self::FactAlreadyRecorded { fact } => json!({ "fact": fact }),
            Self::StepIndexNotAdvancing { current, submitted } => {
                json!({ "current": current, "submitted": submitted })
            }
            Self::CancellationIsBestEffort => json!({}),
            Self::CommandAlreadyApplied { command } => json!({ "command": command }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_discarded_fact_produces_no_event_and_says_why() {
        // Given: a late runner fact on a job that already resolved
        let result: JobCommandResult =
            CommandResult::nothing_happened(CommandWarning::FactDiscardedOnTerminalJob {
                fact: "RunStarted",
                job_status: "CANCELLED",
            });
        // When/Then: history is untouched and the caller may acknowledge the message
        assert!(result.is_empty());
        assert_eq!(
            result.warnings.first().map(CommandWarning::code),
            Some("fact_discarded_on_terminal_job")
        );
    }
}
