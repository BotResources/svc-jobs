use crate::domain::fleet::dispatch_mode::DispatchMode;
use crate::domain::fleet::run_duration::MaxRunDuration;

/// The two knobs a platform administrator sets on a RunnerType at any time. A
/// change applies to Runs dispatched afterwards: a Run carries the maximum
/// duration it was dispatched with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunnerTypeSettings {
    pub dispatch_mode: DispatchMode,
    pub max_run_duration: MaxRunDuration,
}

impl RunnerTypeSettings {
    /// The settings a RunnerType is registered with: pushed to, with the
    /// service default maximum duration.
    pub fn registered_with(default_max_run_duration: MaxRunDuration) -> Self {
        Self {
            dispatch_mode: DispatchMode::Push,
            max_run_duration: default_max_run_duration,
        }
    }
}

impl Default for RunnerTypeSettings {
    fn default() -> Self {
        Self::registered_with(MaxRunDuration::SERVICE_DEFAULT)
    }
}
