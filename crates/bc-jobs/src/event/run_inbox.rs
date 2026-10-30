use crate::domain::ids::RunId;
use crate::event::job::JobEvent;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunInboxEffect {
    Untouched,
    JobQueued,
    RetryScheduled,
    RunStarted,
    RunCompleted { run_id: RunId },
    RunFailed,
    RunCancelled { withdrawn: bool },
    JobFailed,
}

impl RunInboxEffect {
    pub fn serializes_with_the_tree_directory(self) -> bool {
        match self {
            Self::JobQueued
            | Self::RetryScheduled
            | Self::RunStarted
            | Self::RunCompleted { .. }
            | Self::RunFailed
            | Self::RunCancelled { .. }
            | Self::JobFailed => true,
            Self::Untouched => false,
        }
    }
}

impl JobEvent {
    pub fn run_inbox_effect(&self) -> RunInboxEffect {
        match self {
            Self::JobQueued(_) => RunInboxEffect::JobQueued,
            Self::RetryScheduled(_) => RunInboxEffect::RetryScheduled,
            Self::RunStarted(_) => RunInboxEffect::RunStarted,
            Self::RunCompleted(fact) => RunInboxEffect::RunCompleted {
                run_id: fact.run_id,
            },
            Self::RunFailed(_) => RunInboxEffect::RunFailed,
            Self::RunCancelled(fact) => RunInboxEffect::RunCancelled {
                withdrawn: fact.withdrawn_for.is_some(),
            },
            Self::JobFailed(_) => RunInboxEffect::JobFailed,
            Self::RunDispatched(_)
            | Self::RunPlanDeclared(_)
            | Self::RunStepStarted(_)
            | Self::RunCancellationRequested(_)
            | Self::JobCompleted(_)
            | Self::JobCancelled(_)
            | Self::ManualRetryStarted(_)
            | Self::JobDeleted(_)
            | Self::JobAffordancesChanged(_)
            | Self::JobPriorityForced(_)
            | Self::JobPriorityRecalculated(_) => RunInboxEffect::Untouched,
        }
    }
}

#[cfg(test)]
pub(crate) mod tests;
