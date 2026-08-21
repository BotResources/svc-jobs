pub mod decision;
pub mod load;
mod presence;
mod projection;

use async_trait::async_trait;
use bc_jobs::domain::actions::fleet::{RetirementWindow, RunnerTypeDecisionFacts};
use bc_jobs::domain::fleet::RunnerType;
use bc_jobs::domain::ids::PresenceSessionId;
use bc_jobs::domain::keys::RunnerTypeKey;
use bc_jobs::ports::PortError;
use bc_jobs::ports::fleet::{
    ClosedPresenceSession, FleetProjectionSource, FleetReader, OpenPresenceSession,
};

use crate::db::PgStore;
use crate::db::hydrate::unavailable;

#[async_trait]
impl FleetReader for PgStore {
    async fn load(&self, key: &RunnerTypeKey) -> Result<Option<RunnerType>, PortError> {
        let mut connection = self.pool().acquire().await.map_err(unavailable)?;
        load::load_one(&mut connection, key).await
    }

    async fn load_all(&self) -> Result<Vec<RunnerType>, PortError> {
        let mut connection = self.pool().acquire().await.map_err(unavailable)?;
        load::load_types(&mut connection, None, load::CorruptionPolicy::FailTheLoad).await
    }

    async fn open_presence_sessions(&self) -> Result<Vec<OpenPresenceSession>, PortError> {
        presence::open_sessions(self).await
    }

    async fn closed_presence_session(
        &self,
        session_id: PresenceSessionId,
    ) -> Result<Option<ClosedPresenceSession>, PortError> {
        presence::closed_session(self, session_id).await
    }

    async fn decision_facts(
        &self,
        key: &RunnerTypeKey,
        window: RetirementWindow,
    ) -> Result<RunnerTypeDecisionFacts, PortError> {
        let mut connection = self.pool().acquire().await.map_err(unavailable)?;
        decision::facts_of(&mut connection, key, window).await
    }

    async fn projection_source(
        &self,
        key: Option<&RunnerTypeKey>,
        window: RetirementWindow,
    ) -> Result<FleetProjectionSource, PortError> {
        projection::projection_source(self, key, window).await
    }
}
