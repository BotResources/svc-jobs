use async_trait::async_trait;
use br_core_events::EventMetadata;
use chrono::{DateTime, Utc};

use crate::domain::actions::fleet::{RetirementWindow, RunnerTypeDecisionFacts};
use crate::domain::fleet::RunnerType;
use crate::domain::ids::PresenceSessionId;
use crate::domain::job::Job;
use crate::domain::keys::{InstanceKey, RunnerTypeKey};
use crate::event::fleet::FleetEvent;
use crate::ports::PortError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenPresenceSession {
    pub runner_type: RunnerTypeKey,
    pub instance_key: InstanceKey,
    pub session_id: PresenceSessionId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClosedPresenceSession {
    pub connected_at: DateTime<Utc>,
    pub disconnected_at: DateTime<Utc>,
}

pub struct DecidableRunnerType {
    pub runner_type: RunnerType,
    pub decision_facts: RunnerTypeDecisionFacts,
}

pub struct FleetProjectionSource {
    pub runner_types: Vec<DecidableRunnerType>,
    pub active_jobs: Vec<Job>,
}

#[async_trait]
pub trait FleetReader: Send + Sync {
    async fn load(&self, key: &RunnerTypeKey) -> Result<Option<RunnerType>, PortError>;

    async fn load_all(&self) -> Result<Vec<RunnerType>, PortError>;

    async fn open_presence_sessions(&self) -> Result<Vec<OpenPresenceSession>, PortError>;

    async fn closed_presence_session(
        &self,
        session_id: PresenceSessionId,
    ) -> Result<Option<ClosedPresenceSession>, PortError>;

    async fn decision_facts(
        &self,
        key: &RunnerTypeKey,
        window: RetirementWindow,
    ) -> Result<RunnerTypeDecisionFacts, PortError>;

    async fn projection_source(
        &self,
        key: Option<&RunnerTypeKey>,
        window: RetirementWindow,
    ) -> Result<FleetProjectionSource, PortError>;
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

#[async_trait]
pub trait RunnerTypeCatalogWriter: Send + Sync {
    async fn project_current(&self, runner_type: &RunnerType) -> Result<(), PortError>;

    async fn reconcile(&self, runner_types: &[RunnerType]) -> Result<(), PortError>;
}
