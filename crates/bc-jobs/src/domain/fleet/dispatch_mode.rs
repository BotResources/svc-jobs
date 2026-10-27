use serde::{Deserialize, Serialize};

use crate::domain::fleet::run_duration::MaxRunDuration;
use crate::domain::ids::RunId;
use crate::error::JobsError;

pub const PUSH: &str = "PUSH";
pub const CLAIM: &str = "CLAIM";

/// How a RunnerType's waiting Jobs reach its Instances: published on the
/// trigger stream (`PUSH`), or handed to an Instance that claims its next Run
/// (`CLAIM`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DispatchMode {
    #[default]
    Push,
    Claim,
}

impl DispatchMode {
    pub fn from_db_str(value: &str) -> Result<Self, JobsError> {
        match value {
            PUSH => Ok(Self::Push),
            CLAIM => Ok(Self::Claim),
            unknown => Err(JobsError::UnknownEnumValue {
                field: "dispatch_mode",
                value: unknown.to_owned(),
            }),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Push => PUSH,
            Self::Claim => CLAIM,
        }
    }
}

/// How the first Run of a manual-retry successor leaves the ledger, from the
/// dispatch mode of its RunnerType.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SuccessorDispatch {
    /// Dispatched with the intervention, on the trigger stream — even when the
    /// predecessor spent its whole budget.
    Push {
        first_run_id: RunId,
        max_run_duration: MaxRunDuration,
    },
    /// Waiting to be claimed, at its effective priority, like any Job.
    AwaitClaim,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_stored_dispatch_mode_has_a_total_mapping() {
        // Given: the two stored codes and one this version does not know
        // When/Then: both round-trip and the unknown one fails loud
        for mode in [DispatchMode::Push, DispatchMode::Claim] {
            assert_eq!(DispatchMode::from_db_str(mode.as_str()), Ok(mode));
        }
        assert!(DispatchMode::from_db_str("PULL").is_err());
    }

    #[test]
    fn a_new_runner_type_is_pushed_to() {
        // Given/When/Then: the default mode is the one every type had before claims existed
        assert_eq!(DispatchMode::default(), DispatchMode::Push);
    }
}
