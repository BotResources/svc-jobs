use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum JobsError {
    #[error("not_uuid_v7")]
    NotUuidV7 { field: &'static str, value: Uuid },
    #[error("blank_value")]
    BlankValue { field: &'static str },
    #[error("invalid_segment")]
    InvalidSegment { field: &'static str, value: String },
    #[error("value_too_long")]
    ValueTooLong {
        field: &'static str,
        length: usize,
        maximum: usize,
    },
    #[error("invalid_jitter")]
    InvalidJitter,
    #[error("invalid_duration")]
    InvalidDuration { field: &'static str },
    #[error("job_id_conflict")]
    JobIdConflict { job_id: Uuid },
    #[error("source_already_active")]
    SourceAlreadyActive { active_job_id: Uuid },
    #[error("parent_job_unknown")]
    ParentJobUnknown { parent_job_id: Uuid },
    #[error("parent_job_terminal")]
    ParentJobTerminal { parent_job_id: Uuid },
    #[error("parent_job_deleted")]
    ParentJobDeleted { parent_job_id: Uuid },
    #[error("self_reference")]
    SelfReference { field: &'static str },
    #[error("max_attempts_above_ceiling")]
    MaxAttemptsAboveCeiling { requested: u32, ceiling: u32 },
    #[error("job_already_terminal")]
    JobAlreadyTerminal { status: &'static str },
    #[error("job_deleted")]
    JobDeleted,
    #[error("job_not_terminal")]
    JobNotTerminal { status: &'static str },
    #[error("job_already_deleted")]
    JobAlreadyDeleted,
    #[error("job_not_failed")]
    JobNotFailed { status: &'static str },
    #[error("job_not_in_progress")]
    JobNotInProgress { status: &'static str },
    #[error("not_owner")]
    NotOwner,
    #[error("successor_still_active")]
    SuccessorStillActive { successor_job_id: Uuid },
    #[error("manual_retry_already_started")]
    ManualRetryAlreadyStarted { successor_job_id: Uuid },
    #[error("manual_retry_conflict")]
    ManualRetryConflict { successor_job_id: Uuid },
    #[error("stale_failed_resolution")]
    StaleFailedResolution { current_resolution_id: Uuid },
    #[error("job_still_active")]
    JobStillActive,
    #[error("inactivity_timeout_not_reached")]
    InactivityTimeoutNotReached { idle_since: DateTime<Utc> },
    #[error("run_not_found")]
    RunNotFound { run_id: Uuid },
    #[error("run_already_in_flight")]
    RunAlreadyInFlight { run_id: Uuid },
    #[error("run_within_max_duration")]
    RunWithinMaxDuration { run_id: Uuid },
    #[error("retry_budget_exhausted")]
    RetryBudgetExhausted { attempts: u32, max_attempts: u32 },
    #[error("retry_not_scheduled")]
    RetryNotScheduled,
    #[error("retry_not_due")]
    RetryNotDue { due_at: DateTime<Utc> },
    #[error("empty_plan")]
    EmptyPlan,
    #[error("duplicate_plan_step_index")]
    DuplicatePlanStepIndex { step_index: u32 },
    #[error("instance_not_live")]
    InstanceNotLive { instance_key: String },
    #[error("corrupt_state")]
    CorruptState { reason_code: &'static str },
    #[error("unknown_enum_value")]
    UnknownEnumValue { field: &'static str, value: String },
    #[error("serialization_failed")]
    Serialization { detail: String },
}

impl JobsError {
    pub fn code(&self) -> String {
        self.to_string()
    }

    pub fn params(&self) -> Value {
        match self {
            Self::NotUuidV7 { field, value } => json!({ "field": field, "value": value }),
            Self::BlankValue { field } | Self::InvalidDuration { field } => {
                json!({ "field": field })
            }
            Self::InvalidSegment { field, value } => json!({ "field": field, "value": value }),
            Self::ValueTooLong {
                field,
                length,
                maximum,
            } => json!({ "field": field, "length": length, "maximum": maximum }),
            Self::InvalidJitter
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
            | Self::RunWithinMaxDuration { run_id } => json!({ "runId": run_id }),
            Self::RetryBudgetExhausted {
                attempts,
                max_attempts,
            } => json!({ "attempts": attempts, "maxAttempts": max_attempts }),
            Self::RetryNotDue { due_at } => json!({ "dueAt": due_at }),
            Self::DuplicatePlanStepIndex { step_index } => json!({ "stepIndex": step_index }),
            Self::InstanceNotLive { instance_key } => json!({ "instanceKey": instance_key }),
            Self::CorruptState { reason_code } => json!({ "reasonCode": reason_code }),
            Self::UnknownEnumValue { field, value } => json!({ "field": field, "value": value }),
            Self::Serialization { detail } => json!({ "detail": detail }),
        }
    }
}

impl From<serde_json::Error> for JobsError {
    fn from(error: serde_json::Error) -> Self {
        Self::Serialization {
            detail: error.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_code_is_the_stable_key_never_a_sentence() {
        // Given: a refusal carrying structured params
        let error = JobsError::RetryBudgetExhausted {
            attempts: 3,
            max_attempts: 3,
        };
        // When: the edge asks for its code and params
        // Then: the code is a snake_case key and the data lives in params
        assert_eq!(error.code(), "retry_budget_exhausted");
        assert_eq!(error.params(), json!({ "attempts": 3, "maxAttempts": 3 }));
    }

    #[test]
    fn params_never_repeat_the_code() {
        // Given: an error whose params identify the offending aggregate
        let job_id = Uuid::from_u128(7);
        let error = JobsError::JobIdConflict { job_id };
        // When/Then: params carry the id, the code stays free of interpolation
        assert_eq!(error.code(), "job_id_conflict");
        assert_eq!(error.params(), json!({ "jobId": job_id }));
    }
}
