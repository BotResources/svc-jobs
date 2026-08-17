use async_trait::async_trait;
use br_core_events::EventMetadata;
use chrono::{DateTime, Utc};

use crate::domain::ids::JobId;
use crate::domain::job::Job;
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

#[async_trait]
pub trait DueWorkReader: Send + Sync {
    async fn jobs_awaiting_dispatch(&self, at: DateTime<Utc>) -> Result<Vec<Job>, PortError>;

    async fn jobs_with_outrun_runs(&self, at: DateTime<Utc>) -> Result<Vec<Job>, PortError>;

    async fn jobs_idle_since(&self, at: DateTime<Utc>) -> Result<Vec<Job>, PortError>;
}
