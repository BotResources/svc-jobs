use async_trait::async_trait;

use crate::domain::config::RunnerConfig;
use crate::domain::ids::{JobId, RunId};
use crate::domain::keys::{ReasonCode, RunnerTypeKey};
use crate::ports::PortError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunTrigger {
    pub job_id: JobId,
    pub run_id: RunId,
    pub runner_type: RunnerTypeKey,
    pub config: Option<RunnerConfig>,
}

#[async_trait]
pub trait RunnerTransport: Send + Sync {
    async fn dispatch(&self, trigger: &RunTrigger) -> Result<(), PortError>;

    async fn request_stop(&self, run_id: RunId, reason_code: &ReasonCode) -> Result<(), PortError>;

    async fn withdraw_stop(&self, run_id: RunId) -> Result<(), PortError>;
}
