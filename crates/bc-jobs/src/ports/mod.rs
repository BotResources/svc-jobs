pub mod directory;
pub mod environment;
pub mod fleet;
pub mod inbox;
pub mod job;
pub mod kept_payload;
pub mod log;
pub mod mail;
pub mod restart;
pub mod result;
pub mod saved_state;
pub mod transport;
pub mod user;

use thiserror::Error;

use crate::error::JobsError;

#[derive(Debug, Error)]
pub enum PortError {
    #[error("concurrent_modification")]
    ConcurrentModification,
    #[error("tree_lock_timeout")]
    TreeLockTimeout,
    #[error("dependency_unavailable")]
    Unavailable { detail: String },
    #[error("stored_state_rejected")]
    StoredStateRejected(#[from] JobsError),
}
