use bc_jobs::domain::ids::RunId;
use contract_jobs::runner as wire;

use super::{Jobs, run_facts};
use crate::error::ServiceError;

pub async fn apply_acknowledged_start(jobs: &Jobs, run_id: RunId) -> Result<(), ServiceError> {
    let Some(runner_type) = jobs.store.awaiting_start(run_id).await? else {
        return Ok(());
    };
    let Some(instance_key) = jobs
        .transport
        .acknowledged_start(run_id, &runner_type)
        .await?
    else {
        return Ok(());
    };
    let fact = wire::RunStarted {
        version: wire::WIRE_VERSION,
        run_id: run_id.as_uuid(),
        instance_key: instance_key.as_str().to_owned(),
    };
    match run_facts::started(jobs, &runner_type, &fact).await {
        Ok(()) | Err(ServiceError::Contended) => Ok(()),
        Err(error) => Err(error),
    }
}
