use async_trait::async_trait;

use super::PortError;
use crate::domain::ids::{JobId, RunId};

#[async_trait]
pub trait RestartWakes: Send + Sync {
    async fn due(&self) -> Result<Vec<RunId>, PortError>;

    async fn drop_wake(&self, origin_run: RunId) -> Result<(), PortError>;

    async fn origin_job_of(&self, origin_run: RunId) -> Result<Option<JobId>, PortError>;
}
