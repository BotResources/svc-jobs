pub mod environment;
pub mod fleet;
pub mod job;
pub mod log;
pub mod transport;

use thiserror::Error;

use crate::error::JobsError;

#[derive(Debug, Error)]
pub enum PortError {
    #[error("concurrent_modification")]
    ConcurrentModification,
    #[error("dependency_unavailable")]
    Unavailable { detail: String },
    #[error("stored_state_rejected")]
    StoredStateRejected(#[from] JobsError),
}
