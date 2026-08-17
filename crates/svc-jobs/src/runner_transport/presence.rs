use std::sync::Arc;

use async_nats::jetstream::kv::Operation;
use contract_jobs::runner as wire;
use futures::StreamExt;

use super::RunnerChannels;
use crate::app::{Jobs, presence};
use crate::error::ServiceError;

pub const PRESENCE_EXPIRED: &str = "presence_expired";
pub const GRACEFUL_SHUTDOWN: &str = "graceful_shutdown";

pub async fn watch(channels: RunnerChannels, jobs: Arc<Jobs>) -> Result<(), ServiceError> {
    let entries = channels
        .presence_bucket()
        .watch_all()
        .await
        .map_err(|error| ServiceError::Infra(error.to_string()))?;
    let mut entries = std::pin::pin!(entries);
    while let Some(entry) = entries.next().await {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                tracing::warn!(error = %error, "the runner presence watch yielded an error");
                continue;
            }
        };
        let outcome = match entry.operation {
            Operation::Put => observed(&jobs, &entry.key, &entry.value).await,
            Operation::Delete => lost(&jobs, &entry.key, GRACEFUL_SHUTDOWN).await,
            Operation::Purge => lost(&jobs, &entry.key, PRESENCE_EXPIRED).await,
        };
        if let Err(error) = outcome {
            tracing::warn!(
                key = %entry.key,
                error = %error,
                "a runner presence signal could not be recorded"
            );
        }
    }
    Ok(())
}

async fn observed(jobs: &Jobs, key: &str, value: &[u8]) -> Result<(), ServiceError> {
    let announced: wire::Presence = match serde_json::from_slice(value) {
        Ok(announced) => announced,
        Err(error) => {
            tracing::warn!(
                key = %key,
                error = %error,
                "a presence entry this service cannot read is ignored; an unknown status code is \
                 never taken for READY"
            );
            return Ok(());
        }
    };
    presence::observed(jobs, &announced).await
}

async fn lost(jobs: &Jobs, key: &str, reason_code: &str) -> Result<(), ServiceError> {
    let Some((runner_type, instance_key)) = key.split_once('.') else {
        return Ok(());
    };
    presence::lost(jobs, runner_type, instance_key, reason_code).await
}
