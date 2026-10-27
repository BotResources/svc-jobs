//! The two offers about RunnerTypes — the catalog (`jobs.runner_type.`) and
//! the dispatch modes (`jobs.dispatch_mode.`) — projected after the commit
//! that changed them and reconciled against the ledger at boot and
//! periodically.

use bc_jobs::domain::keys::RunnerTypeKey;
use bc_jobs::event::fleet::FleetEvent;
use bc_jobs::ports::fleet::FleetReader;

use super::Jobs;
use crate::error::ServiceError;

pub async fn reconcile(jobs: &Jobs) -> Result<(), ServiceError> {
    let runner_types = FleetReader::load_all(&jobs.store).await?;
    jobs.catalog.reconcile(&runner_types).await?;
    jobs.dispatch_modes.reconcile(&runner_types).await?;
    Ok(())
}

pub async fn project_committed(jobs: &Jobs, key: &RunnerTypeKey, events: &[FleetEvent]) {
    let catalog = events.iter().any(FleetEvent::changes_published_catalog);
    let dispatch_mode = events
        .iter()
        .any(FleetEvent::changes_published_dispatch_mode);
    if !catalog && !dispatch_mode {
        return;
    }
    if let Err(error) = project(jobs, key, catalog, dispatch_mode).await {
        tracing::error!(
            runner_type = key.as_str(),
            error = %error,
            "the durable runner-type catalog projection failed after commit; the periodic \
             reconciliation will repair it and the committed mutation remains acknowledged"
        );
    }
}

async fn project(
    jobs: &Jobs,
    key: &RunnerTypeKey,
    catalog: bool,
    dispatch_mode: bool,
) -> Result<(), ServiceError> {
    let Some(runner_type) = FleetReader::load(&jobs.store, key).await? else {
        return Ok(());
    };
    if catalog {
        jobs.catalog.project_current(&runner_type).await?;
    }
    if dispatch_mode {
        jobs.dispatch_modes.project_current(&runner_type).await?;
    }
    Ok(())
}
