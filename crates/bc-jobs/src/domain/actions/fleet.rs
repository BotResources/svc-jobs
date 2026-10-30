use chrono::{DateTime, TimeDelta, Utc};

use crate::domain::actions::{Affordance, Availability};
use crate::domain::fleet::RunnerType;
use crate::domain::fleet::dispatch_mode::{DispatchMode, SuccessorDispatch};
use crate::domain::fleet::instance::RunnerInstance;
use crate::domain::fleet::lifecycle::RunnerTypeLifecycle;
use crate::domain::fleet::run_duration::MaxRunDuration;
use crate::domain::ids::RunId;
use crate::domain::keys::{InstanceKey, RunnerTypeKey};
use crate::error::JobsError;

pub const DISPATCH: &str = "dispatch";
pub const DEPRECATE: &str = "deprecate";
pub const REACTIVATE: &str = "reactivate";
pub const RETIRE: &str = "retire";
pub const SET_DISPATCH_MODE: &str = "set_dispatch_mode";
pub const SET_MAX_RUN_DURATION: &str = "set_max_run_duration";
pub const CLEAR_MAX_RUN_DURATION: &str = "clear_max_run_duration";

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

    pub fn guard_free_capacity(&self, current_runs: u32) -> Result<(), JobsError> {
        if self.free_capacity(current_runs) > 0 {
            Ok(())
        } else {
            Err(JobsError::NoFreeCapacity {
                runner_type: self.key().as_str().to_owned(),
            })
        }
    }

    pub fn guard_push_dispatch(&self) -> Result<(), JobsError> {
        self.guard_dispatch()?;
        match self.dispatch_mode() {
            DispatchMode::Push => Ok(()),
            DispatchMode::Claim => Err(JobsError::NotPushMode {
                runner_type: self.key().as_str().to_owned(),
            }),
        }
    }

    pub fn guard_claim(&self, instance_key: &InstanceKey) -> Result<&RunnerInstance, JobsError> {
        self.guard_accept_job()?;
        if self.dispatch_mode() != DispatchMode::Claim {
            return Err(JobsError::NotClaimMode {
                runner_type: self.key().as_str().to_owned(),
            });
        }
        let live = self
            .instance(instance_key)
            .ok_or_else(|| JobsError::InstanceUnknown {
                instance_key: instance_key.as_str().to_owned(),
            })?;
        if !live.accepts_new_work() {
            return Err(JobsError::InstanceDraining {
                instance_key: instance_key.as_str().to_owned(),
            });
        }
        Ok(live)
    }

    pub fn guard_configure(&self) -> Result<(), JobsError> {
        Ok(())
    }

    pub fn can_configure(&self) -> Availability {
        Availability::from_guard(self.guard_configure())
    }

    pub fn guard_clear_max_run_duration(&self) -> Result<(), JobsError> {
        self.guard_configure()?;
        if self.own_max_run_duration().is_some() {
            Ok(())
        } else {
            Err(JobsError::MaxRunDurationNotSet {
                runner_type: self.key().as_str().to_owned(),
            })
        }
    }

    pub fn can_clear_max_run_duration(&self) -> Availability {
        Availability::from_guard(self.guard_clear_max_run_duration())
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
            Affordance::new(SET_DISPATCH_MODE, self.can_configure()),
            Affordance::new(SET_MAX_RUN_DURATION, self.can_configure()),
            Affordance::new(CLEAR_MAX_RUN_DURATION, self.can_clear_max_run_duration()),
        ]
    }
}

pub fn successor_dispatch(
    runner_type: Option<&RunnerType>,
    first_run_id: RunId,
    service_default: MaxRunDuration,
    current_runs: u32,
) -> SuccessorDispatch {
    match runner_type {
        Some(known)
            if known.dispatch_mode() == DispatchMode::Claim
                || known.free_capacity(current_runs) == 0 =>
        {
            SuccessorDispatch::Wait
        }
        known => SuccessorDispatch::Push {
            first_run_id,
            max_run_duration: known.map_or(service_default, |known| {
                known.max_run_duration_or(service_default)
            }),
        },
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
