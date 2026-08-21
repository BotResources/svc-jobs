use bc_jobs::domain::keys::RunnerTypeKey;
use bc_jobs::event::fleet::FleetEvent;
use bc_jobs::ports::fleet::FleetReader;

use super::Jobs;
use crate::error::ServiceError;

pub async fn reconcile(jobs: &Jobs) -> Result<(), ServiceError> {
    let runner_types = FleetReader::load_all(&jobs.store).await?;
    jobs.catalog.reconcile(&runner_types).await?;
    Ok(())
}

pub async fn project_committed(jobs: &Jobs, key: &RunnerTypeKey, events: &[FleetEvent]) {
    if !events.iter().any(FleetEvent::changes_published_catalog) {
        return;
    }
    if let Err(error) = project(jobs, key).await {
        tracing::error!(
            runner_type = key.as_str(),
            error = %error,
            "the durable runner-type catalog projection failed after commit; the periodic \
             reconciliation will repair it and the committed mutation remains acknowledged"
        );
    }
}

async fn project(jobs: &Jobs, key: &RunnerTypeKey) -> Result<(), ServiceError> {
    let Some(runner_type) = FleetReader::load(&jobs.store, key).await? else {
        return Ok(());
    };
    jobs.catalog.project_current(&runner_type).await?;
    Ok(())
}
