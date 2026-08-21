use bc_jobs::JobsError;
use bc_jobs::ports::PortError;
use br_util_graphql::EdgeError;

use crate::error::ServiceError;

pub fn edge_error(error: impl Into<ServiceError>) -> async_graphql::Error {
    of_service(error.into()).into()
}

pub fn of_service(error: ServiceError) -> EdgeError {
    match error {
        ServiceError::Domain(domain) => of_domain(&domain),
        ServiceError::JobNotFound => EdgeError::not_found().with_reason("job_not_found"),
        ServiceError::RunnerTypeNotFound => {
            EdgeError::not_found().with_reason("runner_type_not_found")
        }
        ServiceError::Contended => EdgeError::conflict().with_reason("concurrent_modification"),
        ServiceError::Infra(detail) => EdgeError::internal(detail),
        other => EdgeError::internal(other.to_string()),
    }
}

pub fn of_port(error: PortError) -> EdgeError {
    match error {
        PortError::StoredStateRejected(domain) => of_domain(&domain),
        PortError::ConcurrentModification => {
            EdgeError::conflict().with_reason("concurrent_modification")
        }
        PortError::Unavailable { detail } => EdgeError::internal(detail),
    }
}

pub fn of_domain(error: &JobsError) -> EdgeError {
    let base = match error {
        JobsError::NotUuidV7 { .. }
        | JobsError::BlankValue { .. }
        | JobsError::InvalidSegment { .. }
        | JobsError::ValueTooLong { .. }
        | JobsError::InvalidDuration { .. }
        | JobsError::OutOfRange { .. }
        | JobsError::InvalidJitter
        | JobsError::InvalidRetryFactor
        | JobsError::NotAJsonObject { .. }
        | JobsError::EmptyPlan
        | JobsError::DuplicatePlanStepIndex { .. }
        | JobsError::MaxAttemptsAboveCeiling { .. } => EdgeError::bad_user_input(),

        JobsError::JobIdConflict { .. }
        | JobsError::SourceAlreadyActive { .. }
        | JobsError::SelfReference { .. }
        | JobsError::SuccessorStillActive { .. }
        | JobsError::ManualRetryAlreadyStarted { .. }
        | JobsError::ManualRetryConflict { .. }
        | JobsError::StaleFailedResolution { .. }
        | JobsError::JobAlreadyDeleted
        | JobsError::RunAlreadyInFlight { .. } => EdgeError::conflict(),

        JobsError::JobAlreadyTerminal { .. }
        | JobsError::JobNotTerminal { .. }
        | JobsError::JobNotFailed { .. }
        | JobsError::JobNotInProgress { .. }
        | JobsError::JobDeleted
        | JobsError::JobStillActive
        | JobsError::InactivityTimeoutNotReached { .. }
        | JobsError::ParentJobTerminal { .. }
        | JobsError::ParentJobDeleted { .. }
        | JobsError::RetryBudgetExhausted { .. }
        | JobsError::RetryNotScheduled
        | JobsError::RetryNotDue { .. }
        | JobsError::RunNotStarted { .. }
        | JobsError::RunWithinMaxDuration { .. }
        | JobsError::RunnerTypeUnavailable { .. }
        | JobsError::RunnerTypeRetired { .. }
        | JobsError::RunnerTypeNotActive { .. }
        | JobsError::RunnerTypeNotDeprecated { .. }
        | JobsError::RunnerTypeAlreadyActive
        | JobsError::RunnerTypeHasNonTerminalJobs { .. }
        | JobsError::RunnerTypeHasRecentTerminalRuns { .. }
        | JobsError::RunnerTypeHasNoLiveInstances
        | JobsError::RunnerTypeMismatch { .. }
        | JobsError::InstanceNotLive { .. }
        | JobsError::StaleLoss { .. } => EdgeError::invalid_state(),

        JobsError::RunNotFound { .. } | JobsError::ParentJobUnknown { .. } => {
            EdgeError::not_found()
        }

        JobsError::NotOwner => EdgeError::forbidden(),

        JobsError::CorruptState { .. }
        | JobsError::UnknownEnumValue { .. }
        | JobsError::Serialization { .. } => EdgeError::internal(error.code()),
    };
    let named = base.with_reason(error.code());
    if params_name_internal_state(error) {
        return named;
    }
    with_params(named, error)
}

fn params_name_internal_state(error: &JobsError) -> bool {
    matches!(error, JobsError::StaleLoss { .. })
}

fn with_params(mut edge: EdgeError, error: &JobsError) -> EdgeError {
    if let Some(fields) = error.params().as_object() {
        for (key, value) in fields {
            edge = edge.with_param(
                key.clone(),
                match value {
                    serde_json::Value::String(text) => text.clone(),
                    other => other.to_string(),
                },
            );
        }
    }
    edge
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn a_stale_loss_reaches_the_edge_without_naming_a_presence_session() {
        // Given: a loss refused because the instance is live again under a new session
        let error = JobsError::StaleLoss {
            instance_key: "pod-7".to_owned(),
            observed_session_id: Uuid::now_v7(),
            live_session_id: Uuid::now_v7(),
        };
        // When: it is rendered for a client
        let edge = of_domain(&error);
        // Then: the code travels, the internal session identifiers do not
        assert_eq!(edge.reason_code(), Some("stale_loss"));
        assert!(edge.params().is_empty());
    }

    #[test]
    fn an_ordinary_refusal_still_carries_the_params_its_code_needs() {
        // Given: a retry refused because the budget is spent
        let error = JobsError::RetryBudgetExhausted {
            attempts: 3,
            max_attempts: 3,
        };
        // When: it is rendered for a client
        let edge = of_domain(&error);
        // Then: the parameters the message needs are still there
        assert_eq!(edge.reason_code(), Some("retry_budget_exhausted"));
        assert_eq!(edge.params().get("attempts").map(String::as_str), Some("3"));
    }
}
