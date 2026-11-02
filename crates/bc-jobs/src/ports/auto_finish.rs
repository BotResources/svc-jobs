use async_trait::async_trait;

use crate::domain::auto_finish::AutoFinishFacts;
use crate::domain::ids::{JobId, RunId};
use crate::ports::PortError;

#[async_trait]
pub trait AutoFinishReader: Send + Sync {
    async fn facts_of(&self, called: JobId) -> Result<Option<AutoFinishFacts>, PortError>;

    async fn candidates_after(
        &self,
        after: Option<JobId>,
        limit: usize,
    ) -> Result<Vec<JobId>, PortError>;

    async fn called_by_runs(&self, runs: &[RunId]) -> Result<Vec<JobId>, PortError>;

    async fn called_jobs_stemming_to(&self, jobs: &[JobId]) -> Result<Vec<JobId>, PortError>;
}
