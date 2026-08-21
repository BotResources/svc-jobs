use serde::{Deserialize, Serialize};

use crate::error::JobsError;

pub const ACTIVE: &str = "ACTIVE";
pub const DEPRECATED: &str = "DEPRECATED";
pub const RETIRED: &str = "RETIRED";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RunnerTypeLifecycle {
    Active,
    Deprecated,
    Retired,
}

impl RunnerTypeLifecycle {
    pub fn from_db_str(value: &str) -> Result<Self, JobsError> {
        match value {
            ACTIVE => Ok(Self::Active),
            DEPRECATED => Ok(Self::Deprecated),
            RETIRED => Ok(Self::Retired),
            unknown => Err(JobsError::UnknownEnumValue {
                field: "runner_type_lifecycle",
                value: unknown.to_owned(),
            }),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Active => ACTIVE,
            Self::Deprecated => DEPRECATED,
            Self::Retired => RETIRED,
        }
    }

    pub fn accepts_jobs(self) -> bool {
        matches!(self, Self::Active | Self::Deprecated)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn persisted_lifecycle_codes_have_a_total_mapping() {
        assert_eq!(
            RunnerTypeLifecycle::from_db_str(ACTIVE),
            Ok(RunnerTypeLifecycle::Active)
        );
        assert_eq!(
            RunnerTypeLifecycle::from_db_str(DEPRECATED),
            Ok(RunnerTypeLifecycle::Deprecated)
        );
        assert_eq!(
            RunnerTypeLifecycle::from_db_str(RETIRED),
            Ok(RunnerTypeLifecycle::Retired)
        );
        assert!(RunnerTypeLifecycle::from_db_str("PAUSED").is_err());
    }
}
