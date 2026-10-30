use serde::{Deserialize, Serialize};

use crate::domain::fleet::run_duration::MaxRunDuration;
use crate::domain::ids::RunId;
use crate::error::JobsError;

pub const PUSH: &str = "PUSH";
pub const CLAIM: &str = "CLAIM";

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SuccessorDispatch {
    Push {
        first_run_id: RunId,
        max_run_duration: MaxRunDuration,
    },
    Wait,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_stored_dispatch_mode_has_a_total_mapping() {
        for mode in [DispatchMode::Push, DispatchMode::Claim] {
            assert_eq!(DispatchMode::from_db_str(mode.as_str()), Ok(mode));
        }
        assert!(DispatchMode::from_db_str("PULL").is_err());
    }

    #[test]
    fn a_new_runner_type_is_pushed_to() {
        assert_eq!(DispatchMode::default(), DispatchMode::Push);
    }
}
