use bc_jobs::JobsError;
use bc_jobs::ports::PortError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ServiceError {
    #[error("missing_configuration: {key}")]
    MissingConfiguration { key: &'static str },

    #[error("invalid_configuration: {key}={value}")]
    Configuration { key: &'static str, value: String },

    #[error(transparent)]
    Domain(#[from] JobsError),

    #[error("job_not_found")]
    JobNotFound,

    #[error("runner_type_not_found")]
    RunnerTypeNotFound,

    #[error("concurrent_modification")]
    Contended,

    #[error("infrastructure_failure: {0}")]
    Infra(String),
}

impl From<PortError> for ServiceError {
    fn from(error: PortError) -> Self {
        match error {
            PortError::StoredStateRejected(domain) => Self::Domain(domain),
            PortError::ConcurrentModification => Self::Contended,
            other => Self::Infra(other.to_string()),
        }
    }
}

impl From<sqlx::Error> for ServiceError {
    fn from(error: sqlx::Error) -> Self {
        Self::Infra(error.to_string())
    }
}

impl From<serde_json::Error> for ServiceError {
    fn from(error: serde_json::Error) -> Self {
        Self::Domain(JobsError::from(error))
    }
}

impl From<br_util_nats_fabric::FabricError> for ServiceError {
    fn from(error: br_util_nats_fabric::FabricError) -> Self {
        Self::Infra(error.to_string())
    }
}
