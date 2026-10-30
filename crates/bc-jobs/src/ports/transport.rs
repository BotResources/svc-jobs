use std::collections::HashSet;

use async_trait::async_trait;

use crate::domain::attempts::AttemptNumber;
use crate::domain::config::RunnerConfig;
use crate::domain::ids::{JobId, RunId};
use crate::domain::job::{Job, JobCall};
use crate::domain::keys::{InstanceKey, ReasonCode, RunnerTypeKey};
use crate::domain::passport::JobPassport;
use crate::domain::payload::JobPayload;
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
    pub parent_job_id: JobId,
    pub initial_job_id: JobId,
    pub passport: Option<JobPassport>,
    pub payload: Option<JobPayload>,
    pub call: Option<JobCall>,
    pub restart_of_run_id: Option<RunId>,
}

impl RunTrigger {
    pub fn of_job(job: &Job, run_id: RunId, attempt: AttemptNumber) -> Self {
        Self {
            job_id: job.id(),
            run_id,
            runner_type: job.runner_type().clone(),
            config: job.config().cloned(),
            attempt,
            triggered_by: job.triggered_by().cloned(),
            parent_job_id: job.wire_parent_job_id(),
            initial_job_id: job.initial_job_id(),
            passport: job.passport().cloned(),
            payload: job.payload().cloned(),
            call: job.call().copied(),
            restart_of_run_id: None,
        }
    }
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

    async fn acknowledged_start(
        &self,
        run_id: RunId,
        runner_type: &RunnerTypeKey,
    ) -> Result<Option<InstanceKey>, PortError>;
}
