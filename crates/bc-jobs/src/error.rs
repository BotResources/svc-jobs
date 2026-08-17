use std::fmt;

use chrono::{DateTime, Utc};
use serde_json::error::Category;
use serde_json::{Value, json};
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

impl JobsError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::NotUuidV7 { .. } => "not_uuid_v7",
            Self::BlankValue { .. } => "blank_value",
            Self::InvalidSegment { .. } => "invalid_segment",
            Self::ValueTooLong { .. } => "value_too_long",
            Self::InvalidJitter => "invalid_jitter",
            Self::InvalidRetryFactor => "invalid_retry_factor",
            Self::InvalidDuration { .. } => "invalid_duration",
            Self::OutOfRange { .. } => "out_of_range",
            Self::NotAJsonObject { .. } => "not_a_json_object",
            Self::JobIdConflict { .. } => "job_id_conflict",
            Self::SourceAlreadyActive { .. } => "source_already_active",
            Self::ParentJobUnknown { .. } => "parent_job_unknown",
            Self::ParentJobTerminal { .. } => "parent_job_terminal",
            Self::ParentJobDeleted { .. } => "parent_job_deleted",
            Self::SelfReference { .. } => "self_reference",
            Self::MaxAttemptsAboveCeiling { .. } => "max_attempts_above_ceiling",
            Self::JobAlreadyTerminal { .. } => "job_already_terminal",
            Self::JobDeleted => "job_deleted",
            Self::JobNotTerminal { .. } => "job_not_terminal",
            Self::JobAlreadyDeleted => "job_already_deleted",
            Self::JobNotFailed { .. } => "job_not_failed",
            Self::JobNotInProgress { .. } => "job_not_in_progress",
            Self::NotOwner => "not_owner",
            Self::SuccessorStillActive { .. } => "successor_still_active",
            Self::ManualRetryAlreadyStarted { .. } => "manual_retry_already_started",
            Self::ManualRetryConflict { .. } => "manual_retry_conflict",
            Self::StaleFailedResolution { .. } => "stale_failed_resolution",
            Self::JobStillActive => "job_still_active",
            Self::InactivityTimeoutNotReached { .. } => "inactivity_timeout_not_reached",
            Self::RunNotFound { .. } => "run_not_found",
            Self::RunAlreadyInFlight { .. } => "run_already_in_flight",
            Self::RunNotStarted { .. } => "run_not_started",
            Self::RunWithinMaxDuration { .. } => "run_within_max_duration",
            Self::RunnerTypeMismatch { .. } => "runner_type_mismatch",
            Self::RunnerTypeUnavailable { .. } => "runner_type_unavailable",
            Self::RetryBudgetExhausted { .. } => "retry_budget_exhausted",
            Self::RetryNotScheduled => "retry_not_scheduled",
            Self::RetryNotDue { .. } => "retry_not_due",
            Self::EmptyPlan => "empty_plan",
            Self::DuplicatePlanStepIndex { .. } => "duplicate_plan_step_index",
            Self::InstanceNotLive { .. } => "instance_not_live",
            Self::StaleLoss { .. } => "stale_loss",
            Self::CorruptState { .. } => "corrupt_state",
            Self::UnknownEnumValue { .. } => "unknown_enum_value",
            Self::Serialization { .. } => "serialization_failed",
        }
    }

    pub fn params(&self) -> Value {
        match self {
            Self::NotUuidV7 { field, value } => json!({ "field": field, "value": value }),
            Self::BlankValue { field }
            | Self::InvalidDuration { field }
            | Self::NotAJsonObject { field } => {
                json!({ "field": field })
            }
            Self::InvalidSegment { field, value } => json!({ "field": field, "value": value }),
            Self::OutOfRange { field, value } => json!({ "field": field, "value": value }),
            Self::ValueTooLong {
                field,
                length,
                maximum,
            } => json!({ "field": field, "length": length, "maximum": maximum }),
            Self::InvalidJitter
            | Self::InvalidRetryFactor
            | Self::JobDeleted
            | Self::JobAlreadyDeleted
            | Self::NotOwner
            | Self::JobStillActive
            | Self::RetryNotScheduled
            | Self::EmptyPlan => json!({}),
            Self::JobIdConflict { job_id } => json!({ "jobId": job_id }),
            Self::SourceAlreadyActive { active_job_id } => json!({ "activeJobId": active_job_id }),
            Self::ParentJobUnknown { parent_job_id }
            | Self::ParentJobTerminal { parent_job_id }
            | Self::ParentJobDeleted { parent_job_id } => json!({ "parentJobId": parent_job_id }),
            Self::SelfReference { field } => json!({ "field": field }),
            Self::MaxAttemptsAboveCeiling { requested, ceiling } => {
                json!({ "requested": requested, "ceiling": ceiling })
            }
            Self::JobAlreadyTerminal { status }
            | Self::JobNotTerminal { status }
            | Self::JobNotFailed { status }
            | Self::JobNotInProgress { status } => json!({ "status": status }),
            Self::SuccessorStillActive { successor_job_id }
            | Self::ManualRetryAlreadyStarted { successor_job_id }
            | Self::ManualRetryConflict { successor_job_id } => {
                json!({ "successorJobId": successor_job_id })
            }
            Self::StaleFailedResolution {
                current_resolution_id,
            } => json!({ "currentResolutionId": current_resolution_id }),
            Self::InactivityTimeoutNotReached { idle_since } => json!({ "idleSince": idle_since }),
            Self::RunNotFound { run_id }
            | Self::RunAlreadyInFlight { run_id }
            | Self::RunNotStarted { run_id }
            | Self::RunWithinMaxDuration { run_id } => json!({ "runId": run_id }),
            Self::RunnerTypeMismatch { expected, claimed } => {
                json!({ "expected": expected, "claimed": claimed })
            }
            Self::RunnerTypeUnavailable { runner_type } => json!({ "runnerType": runner_type }),
            Self::RetryBudgetExhausted {
                attempts,
                max_attempts,
            } => json!({ "attempts": attempts, "maxAttempts": max_attempts }),
            Self::RetryNotDue { due_at } => json!({ "dueAt": due_at }),
            Self::DuplicatePlanStepIndex { step_index } => json!({ "stepIndex": step_index }),
            Self::InstanceNotLive { instance_key } => json!({ "instanceKey": instance_key }),
            Self::StaleLoss {
                instance_key,
                observed_session_id,
                live_session_id,
            } => json!({
                "instanceKey": instance_key,
                "observedSessionId": observed_session_id,
                "liveSessionId": live_session_id,
            }),
            Self::CorruptState { reason_code } => json!({ "reasonCode": reason_code }),
            Self::UnknownEnumValue { field, value } => json!({ "field": field, "value": value }),
            Self::Serialization {
                category,
                line,
                column,
            } => json!({ "category": category, "line": line, "column": column }),
        }
    }
}

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
