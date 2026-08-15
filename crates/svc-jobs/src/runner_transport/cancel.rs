use bc_jobs::domain::ids::RunId;
use bc_jobs::ports::PortError;
use contract_jobs::runner as wire;
use contract_jobs::runner_transport::run_cancel_key;
use uuid::Uuid;

use super::{RunnerChannels, transport_error};

pub async fn request_stop(channels: &RunnerChannels, run_id: RunId) -> Result<(), PortError> {
    let entry = wire::CancelRun {
        version: wire::WIRE_VERSION,
        run_id: run_id.as_uuid(),
    };
    let bytes = serde_json::to_vec(&entry).map_err(transport_error)?;
    channels
        .cancel_bucket()
        .put(run_cancel_key(run_id.as_uuid()), bytes.into())
        .await
        .map_err(transport_error)?;
    Ok(())
}

pub async fn withdraw_stop(channels: &RunnerChannels, run_id: RunId) -> Result<(), PortError> {
    channels
        .cancel_bucket()
        .delete(run_cancel_key(run_id.as_uuid()))
        .await
        .map_err(transport_error)?;
    Ok(())
}

pub async fn standing_requests(channels: &RunnerChannels) -> Result<Vec<Uuid>, PortError> {
    use futures::StreamExt;

    let keys = channels
        .cancel_bucket()
        .keys()
        .await
        .map_err(transport_error)?;
    let mut standing = Vec::new();
    let mut keys = std::pin::pin!(keys);
    while let Some(key) = keys.next().await {
        let key = key.map_err(transport_error)?;
        if let Ok(run_id) = Uuid::parse_str(&key) {
            standing.push(run_id);
        }
    }
    Ok(standing)
}
