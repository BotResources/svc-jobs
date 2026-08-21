use serde_json::{Value, json};

use super::JobsError;

impl JobsError {
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
            Self::RunnerTypeHasNoLiveInstances | Self::RunnerTypeAlreadyActive => json!({}),
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
            Self::RunnerTypeRetired { runner_type } => json!({ "runnerType": runner_type }),
            Self::RunnerTypeNotActive { lifecycle }
            | Self::RunnerTypeNotDeprecated { lifecycle } => json!({ "lifecycle": lifecycle }),
            Self::RunnerTypeHasNonTerminalJobs { count } => json!({ "count": count }),
            Self::RunnerTypeHasRecentTerminalRuns { eligible_at } => {
                json!({ "eligibleAt": eligible_at })
            }
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
