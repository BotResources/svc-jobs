use std::collections::HashSet;

use async_trait::async_trait;

use crate::domain::attempts::AttemptNumber;
use crate::domain::config::RunnerConfig;
use crate::domain::ids::{JobId, RunId};
use crate::domain::keys::{InstanceKey, ReasonCode, RunnerTypeKey};
use crate::domain::references::KnownUser;
use crate::ports::PortError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunTrigger {
    pub job_id: JobId,
    pub run_id: RunId,
    pub runner_type: RunnerTypeKey,
    pub config: Option<RunnerConfig>,
    pub attempt: AttemptNumber,
    pub triggered_by: Option<KnownUser>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AnnouncedInstance {
    pub runner_type: RunnerTypeKey,
    pub instance_key: InstanceKey,
}

#[async_trait]
pub trait RunnerTransport: Send + Sync {
    async fn dispatch(&self, trigger: &RunTrigger) -> Result<(), PortError>;

    async fn request_stop(&self, run_id: RunId, reason_code: &ReasonCode) -> Result<(), PortError>;

    async fn withdraw_stop(&self, run_id: RunId) -> Result<(), PortError>;

    async fn withdraw_trigger(
        &self,
        run_id: RunId,
        runner_type: &RunnerTypeKey,
    ) -> Result<(), PortError>;

    async fn announced_instances(&self) -> Result<HashSet<AnnouncedInstance>, PortError>;
}
