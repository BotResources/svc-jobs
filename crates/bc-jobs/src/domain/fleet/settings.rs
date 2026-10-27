use crate::domain::fleet::dispatch_mode::DispatchMode;
use crate::domain::fleet::run_duration::MaxRunDuration;

use chrono::{DateTime, Utc};

/// The two knobs a platform administrator sets on a RunnerType at any time. A
/// change applies to Runs dispatched afterwards: a Run carries the maximum
/// duration it was dispatched with.
///
/// A RunnerType is registered pushed to and following the service default
/// maximum duration (`max_run_duration` absent) until an administrator sets
/// one of its own. `dispatch_mode_since` is when the mode last changed —
/// absent while the type keeps the mode it was registered with.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RunnerTypeSettings {
    pub dispatch_mode: DispatchMode,
    pub max_run_duration: Option<MaxRunDuration>,
    pub dispatch_mode_since: Option<DateTime<Utc>>,
}
