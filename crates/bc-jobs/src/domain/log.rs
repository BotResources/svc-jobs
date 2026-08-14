use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::domain::ids::{RunId, RunLogId};
use crate::domain::keys::LogMessage;
use crate::domain::run::step::StepIndex;
use crate::domain::wire::db_string_serde;
use crate::error::JobsError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub enum RunLogLevel {
    Info,
    Warning,
    Error,
}

impl RunLogLevel {
    pub fn as_db_str(&self) -> &'static str {
        match self {
            Self::Info => "INFO",
            Self::Warning => "WARNING",
            Self::Error => "ERROR",
        }
    }

    pub fn from_db_str(value: &str) -> Result<Self, JobsError> {
        match value {
            "INFO" => Ok(Self::Info),
            "WARNING" => Ok(Self::Warning),
            "ERROR" => Ok(Self::Error),
            other => Err(JobsError::UnknownEnumValue {
                field: "run_log_level",
                value: other.to_owned(),
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunLogLine {
    id: RunLogId,
    run_id: RunId,
    step_index: Option<StepIndex>,
    level: RunLogLevel,
    message: LogMessage,
    logged_at: DateTime<Utc>,
}

impl RunLogLine {
    pub fn new(
        id: RunLogId,
        run_id: RunId,
        step_index: Option<StepIndex>,
        level: RunLogLevel,
        message: LogMessage,
        logged_at: DateTime<Utc>,
    ) -> Self {
        Self {
            id,
            run_id,
            step_index,
            level,
            message,
            logged_at,
        }
    }

    pub fn id(&self) -> RunLogId {
        self.id
    }

    pub fn run_id(&self) -> RunId {
        self.run_id
    }

    pub fn step_index(&self) -> Option<StepIndex> {
        self.step_index
    }

    pub fn level(&self) -> RunLogLevel {
        self.level
    }

    pub fn message(&self) -> &LogMessage {
        &self.message
    }

    pub fn logged_at(&self) -> DateTime<Utc> {
        self.logged_at
    }
}

db_string_serde!(RunLogLevel);

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn a_log_line_keeps_the_step_the_runner_named_and_infers_none() {
        // Given: a runner logging without naming a step
        let line = RunLogLine::new(
            RunLogId::new(Uuid::now_v7()).unwrap(),
            RunId::new(Uuid::now_v7()).unwrap(),
            None,
            RunLogLevel::Info,
            LogMessage::new("connecting to provider").unwrap(),
            DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
        );
        // When/Then: the association stays absent — it is never guessed from time
        assert_eq!(line.step_index(), None);
    }

    #[test]
    fn an_unknown_stored_level_fails_loud() {
        // Given: a level written by another version of the schema
        // When/Then: it is refused rather than downgraded to INFO
        assert_eq!(
            RunLogLevel::from_db_str("TRACE"),
            Err(JobsError::UnknownEnumValue {
                field: "run_log_level",
                value: "TRACE".to_owned()
            })
        );
    }
}
