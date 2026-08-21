use chrono::{DateTime, TimeDelta, Utc};

use crate::domain::actions::{Affordance, Availability};
use crate::domain::fleet::RunnerType;
use crate::domain::fleet::lifecycle::RunnerTypeLifecycle;
use crate::domain::keys::RunnerTypeKey;
use crate::error::JobsError;

pub const DISPATCH: &str = "dispatch";
pub const DEPRECATE: &str = "deprecate";
pub const REACTIVATE: &str = "reactivate";
pub const RETIRE: &str = "retire";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetirementWindow {
    pub evaluated_at: DateTime<Utc>,
    pub quiet_period: TimeDelta,
}

impl RetirementWindow {
    pub fn opens_at(&self, latest_terminal_run_at: DateTime<Utc>) -> DateTime<Utc> {
        latest_terminal_run_at + self.quiet_period
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunnerTypeDecisionFacts {
    pub non_terminal_job_count: u32,
    pub latest_terminal_run_at: Option<DateTime<Utc>>,
    pub window: RetirementWindow,
}

impl RunnerTypeDecisionFacts {
    pub fn unused(window: RetirementWindow) -> Self {
        Self {
            non_terminal_job_count: 0,
            latest_terminal_run_at: None,
            window,
        }
    }
}

impl RunnerType {
    pub fn guard_accept_job(&self) -> Result<(), JobsError> {
        guard_accept_job(self.lifecycle(), self.key())
    }

    pub fn guard_dispatch(&self) -> Result<(), JobsError> {
        self.guard_accept_job()?;
        if self.is_available() {
            Ok(())
        } else {
            Err(JobsError::RunnerTypeUnavailable {
                runner_type: self.key().as_str().to_owned(),
            })
        }
    }

    pub fn can_dispatch(&self) -> Availability {
        Availability::from_guard(self.guard_dispatch())
    }

    pub fn guard_deprecate(&self) -> Result<(), JobsError> {
        match self.lifecycle() {
            RunnerTypeLifecycle::Active => Ok(()),
            lifecycle => Err(JobsError::RunnerTypeNotActive {
                lifecycle: lifecycle.as_str(),
            }),
        }
    }

    pub fn can_deprecate(&self) -> Availability {
        Availability::from_guard(self.guard_deprecate())
    }

    pub fn guard_reactivate(&self) -> Result<(), JobsError> {
        match self.lifecycle() {
            RunnerTypeLifecycle::Deprecated => Ok(()),
            RunnerTypeLifecycle::Retired if self.instances().is_empty() => {
                Err(JobsError::RunnerTypeHasNoLiveInstances)
            }
            RunnerTypeLifecycle::Retired => Ok(()),
            RunnerTypeLifecycle::Active => Err(JobsError::RunnerTypeAlreadyActive),
        }
    }

    pub fn can_reactivate(&self) -> Availability {
        Availability::from_guard(self.guard_reactivate())
    }

    pub fn guard_retire(&self, facts: RunnerTypeDecisionFacts) -> Result<(), JobsError> {
        if self.lifecycle() != RunnerTypeLifecycle::Deprecated {
            return Err(JobsError::RunnerTypeNotDeprecated {
                lifecycle: self.lifecycle().as_str(),
            });
        }
        if facts.non_terminal_job_count > 0 {
            return Err(JobsError::RunnerTypeHasNonTerminalJobs {
                count: facts.non_terminal_job_count,
            });
        }
        if let Some(last_terminal) = facts.latest_terminal_run_at {
            let eligible_at = facts.window.opens_at(last_terminal);
            if facts.window.evaluated_at < eligible_at {
                return Err(JobsError::RunnerTypeHasRecentTerminalRuns { eligible_at });
            }
        }
        Ok(())
    }

    pub fn can_retire(&self, facts: RunnerTypeDecisionFacts) -> Availability {
        Availability::from_guard(self.guard_retire(facts))
    }

    pub fn affordances(&self, facts: RunnerTypeDecisionFacts) -> Vec<Affordance> {
        vec![
            Affordance::new(DISPATCH, self.can_dispatch()),
            Affordance::new(DEPRECATE, self.can_deprecate()),
            Affordance::new(REACTIVATE, self.can_reactivate()),
            Affordance::new(RETIRE, self.can_retire(facts)),
        ]
    }
}

pub fn guard_accept_job(
    lifecycle: RunnerTypeLifecycle,
    key: &RunnerTypeKey,
) -> Result<(), JobsError> {
    if lifecycle.accepts_jobs() {
        Ok(())
    } else {
        Err(JobsError::RunnerTypeRetired {
            runner_type: key.as_str().to_owned(),
        })
    }
}

#[cfg(test)]
mod tests;
