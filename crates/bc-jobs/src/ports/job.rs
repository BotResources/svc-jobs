use async_trait::async_trait;
use br_core_events::EventMetadata;
use chrono::{DateTime, TimeDelta, Utc};

use crate::domain::ids::JobId;
use crate::domain::job::Job;
use crate::domain::keys::RunnerTypeKey;
use crate::domain::references::SourceReference;
use crate::event::job::JobEvent;
use crate::ports::PortError;

#[async_trait]
pub trait JobReader: Send + Sync {
    async fn load(&self, id: JobId) -> Result<Option<Job>, PortError>;

    async fn load_descendants(&self, id: JobId) -> Result<Vec<Job>, PortError>;

    async fn load_active_for_source(
        &self,
        source: &SourceReference,
    ) -> Result<Option<Job>, PortError>;
}

#[async_trait]
pub trait JobWriter: Send + Sync {
    async fn append(
        &self,
        job_id: JobId,
        events: &[JobEvent],
        metadata: &EventMetadata,
        occurred_at: DateTime<Utc>,
    ) -> Result<(), PortError>;
}

pub const DUE_WORK_BATCH: usize = 200;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DispatchWindow {
    pub runner_type: RunnerTypeKey,
    pub free: u32,
}

#[async_trait]
pub trait DueWorkReader: Send + Sync {
    async fn jobs_awaiting_dispatch(
        &self,
        at: DateTime<Utc>,
        windows: &[DispatchWindow],
    ) -> Result<Vec<Job>, PortError>;

    async fn jobs_with_outrun_runs(
        &self,
        now: DateTime<Utc>,
        service_default: TimeDelta,
    ) -> Result<Vec<Job>, PortError>;

    async fn jobs_with_lost_claims(&self, cutoff: DateTime<Utc>) -> Result<Vec<Job>, PortError>;

    async fn jobs_with_stranded_triggers(
        &self,
        cutoff: DateTime<Utc>,
    ) -> Result<Vec<Job>, PortError>;

    async fn jobs_idle_since(&self, at: DateTime<Utc>) -> Result<Vec<Job>, PortError>;
}
