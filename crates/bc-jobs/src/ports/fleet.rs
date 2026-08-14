use async_trait::async_trait;
use br_core_events::EventMetadata;
use chrono::{DateTime, Utc};

use crate::domain::fleet::RunnerType;
use crate::domain::keys::RunnerTypeKey;
use crate::event::fleet::FleetEvent;
use crate::ports::PortError;

#[async_trait]
pub trait FleetReader: Send + Sync {
    async fn load(&self, key: &RunnerTypeKey) -> Result<Option<RunnerType>, PortError>;

    async fn load_all(&self) -> Result<Vec<RunnerType>, PortError>;
}

#[async_trait]
pub trait FleetWriter: Send + Sync {
    async fn append(
        &self,
        events: &[FleetEvent],
        metadata: &EventMetadata,
        occurred_at: DateTime<Utc>,
    ) -> Result<(), PortError>;
}
