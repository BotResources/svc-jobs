use crate::domain::fleet::dispatch_mode::DispatchMode;
use crate::domain::fleet::run_duration::MaxRunDuration;

/// The two knobs a platform administrator sets on a RunnerType at any time. A
/// change applies to Runs dispatched afterwards: a Run carries the maximum
/// duration it was dispatched with.
///
/// A RunnerType is registered pushed to and following the service default
/// maximum duration (`max_run_duration` absent) until an administrator sets
/// one of its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RunnerTypeSettings {
    pub dispatch_mode: DispatchMode,
    pub max_run_duration: Option<MaxRunDuration>,
}
