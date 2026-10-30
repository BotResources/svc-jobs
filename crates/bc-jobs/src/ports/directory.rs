use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::domain::directory::listing::{DirectoryEntry, RestartObstacles};
use crate::domain::directory::registration::{
    AgentDeclaration, HeldAgent, Registration, RegistrationRefusal, RunStanding,
};
use crate::domain::ids::{JobId, RunId};
use crate::ports::PortError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedEntry {
    pub entry: DirectoryEntry,
    pub obstacles: RestartObstacles,
}

pub struct RunCaller<'a> {
    pub run_id: RunId,
    pub runner_type: &'a str,
    pub instance_key: &'a str,
}

pub type RegistrationDecision<'a> =
    &'a (dyn Fn(RunStanding, &[HeldAgent]) -> Result<Registration, RegistrationRefusal> + Sync);

pub type StandingDecision<'a> =
    &'a (dyn Fn(RunStanding) -> Result<JobId, RegistrationRefusal> + Sync);

#[async_trait]
pub trait AgentDirectory: Send + Sync {
    async fn register(
        &self,
        caller: RunCaller<'_>,
        declarations: &[AgentDeclaration],
        decide: RegistrationDecision<'_>,
        at: DateTime<Utc>,
    ) -> Result<Result<(), RegistrationRefusal>, PortError>;

    async fn list(
        &self,
        caller: RunCaller<'_>,
        decide: StandingDecision<'_>,
    ) -> Result<Result<Vec<ListedEntry>, RegistrationRefusal>, PortError>;
}

#[async_trait]
pub trait TreeDirectory: Send + Sync {
    async fn entries_of_tree(&self, job_id: JobId) -> Result<Option<Vec<ListedEntry>>, PortError>;
}
