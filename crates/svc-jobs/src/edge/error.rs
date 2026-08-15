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
        PortError::Refused { detail } | PortError::Unavailable { detail } => {
            EdgeError::internal(detail)
        }
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
        | JobsError::RunnerTypeMismatch { .. }
        | JobsError::InstanceNotLive { .. } => EdgeError::invalid_state(),

        JobsError::RunNotFound { .. } | JobsError::ParentJobUnknown { .. } => {
            EdgeError::not_found()
        }

        JobsError::NotOwner => EdgeError::forbidden(),

        JobsError::CorruptState { .. }
        | JobsError::UnknownEnumValue { .. }
        | JobsError::Serialization { .. } => EdgeError::internal(error.code()),
    };
    with_params(base.with_reason(error.code()), error)
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
