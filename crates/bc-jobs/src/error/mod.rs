mod code;
mod params;

use std::fmt;

use chrono::{DateTime, Utc};
use serde_json::error::Category;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobsError {
    NotUuidV7 {
        field: &'static str,
        value: Uuid,
    },
    BlankValue {
        field: &'static str,
    },
    InvalidSegment {
        field: &'static str,
        value: String,
    },
    ValueTooLong {
        field: &'static str,
        length: usize,
        maximum: usize,
    },
    InvalidJitter,
    InvalidRetryFactor,
    InvalidDuration {
        field: &'static str,
    },
    OutOfRange {
        field: &'static str,
        value: i64,
    },
    NotAJsonObject {
        field: &'static str,
    },
    JobIdConflict {
        job_id: Uuid,
    },
    SourceAlreadyActive {
        active_job_id: Uuid,
    },
    ParentJobUnknown {
        parent_job_id: Uuid,
    },
    ParentJobTerminal {
        parent_job_id: Uuid,
    },
    ParentJobDeleted {
        parent_job_id: Uuid,
    },
    SelfReference {
        field: &'static str,
    },
    MaxAttemptsAboveCeiling {
        requested: u32,
        ceiling: u32,
    },
    JobAlreadyTerminal {
        status: &'static str,
    },
    JobDeleted,
    JobNotTerminal {
        status: &'static str,
    },
    JobAlreadyDeleted,
    JobNotFailed {
        status: &'static str,
    },
    JobNotInProgress {
        status: &'static str,
    },
    NotOwner,
    SuccessorStillActive {
        successor_job_id: Uuid,
    },
    ManualRetryAlreadyStarted {
        successor_job_id: Uuid,
    },
    ManualRetryConflict {
        successor_job_id: Uuid,
    },
    StaleFailedResolution {
        current_resolution_id: Uuid,
    },
    JobStillActive,
    InactivityTimeoutNotReached {
        idle_since: DateTime<Utc>,
    },
    RunNotFound {
        run_id: Uuid,
    },
    RunAlreadyInFlight {
        run_id: Uuid,
    },
    RunNotStarted {
        run_id: Uuid,
    },
    RunWithinMaxDuration {
        run_id: Uuid,
    },
    RunnerTypeMismatch {
        expected: String,
        claimed: String,
    },
    RunnerTypeUnavailable {
        runner_type: String,
    },
    RunnerTypeRetired {
        runner_type: String,
    },
    RunnerTypeNotActive {
        lifecycle: &'static str,
    },
    RunnerTypeNotDeprecated {
        lifecycle: &'static str,
    },
    RunnerTypeAlreadyActive,
    RunnerTypeHasNonTerminalJobs {
        count: u32,
    },
    RunnerTypeHasRecentTerminalRuns {
        eligible_at: DateTime<Utc>,
    },
    RunnerTypeHasNoLiveInstances,
    RetryBudgetExhausted {
        attempts: u32,
        max_attempts: u32,
    },
    RetryNotScheduled,
    RetryNotDue {
        due_at: DateTime<Utc>,
    },
    EmptyPlan,
    DuplicatePlanStepIndex {
        step_index: u32,
    },
    InstanceNotLive {
        instance_key: String,
    },
    StaleLoss {
        instance_key: String,
        observed_session_id: Uuid,
        live_session_id: Uuid,
    },
    CorruptState {
        reason_code: &'static str,
    },
    UnknownEnumValue {
        field: &'static str,
        value: String,
    },
    Serialization {
        category: &'static str,
        line: usize,
        column: usize,
    },
}

impl fmt::Display for JobsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code())
    }
}

impl std::error::Error for JobsError {}

impl From<serde_json::Error> for JobsError {
    fn from(error: serde_json::Error) -> Self {
        Self::Serialization {
            category: match error.classify() {
                Category::Io => "io",
                Category::Syntax => "syntax",
                Category::Data => "data",
                Category::Eof => "eof",
            },
            line: error.line(),
            column: error.column(),
        }
    }
}

#[cfg(test)]
mod tests;
