use std::sync::Arc;

use async_nats::jetstream::kv::Operation;
use bc_jobs::domain::keys::{InstanceKey, RunnerTypeKey};
use bc_jobs::ports::PortError;
use contract_jobs::runner as wire;
use contract_jobs::runner_transport::runner_presence_key;
use contract_jobs::segment::SubjectSegment;
use futures::StreamExt;

use super::{RunnerChannels, transport_error};
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
                "a presence entry this service cannot read is never taken for READY: a live \
                 instance is drained so it takes no new work while keeping the runs it holds, \
                 and an entry naming no live instance is ignored"
            );
            return unreadable(jobs, key).await;
        }
    };
    presence::observed(jobs, &announced).await
}

async fn unreadable(jobs: &Jobs, key: &str) -> Result<(), ServiceError> {
    let Some((runner_type, instance_key)) = key.split_once('.') else {
        return Ok(());
    };
    presence::drained(jobs, runner_type, instance_key).await
}

async fn lost(jobs: &Jobs, key: &str, reason_code: &str) -> Result<(), ServiceError> {
    let Some((runner_type, instance_key)) = key.split_once('.') else {
        return Ok(());
    };
    presence::lost(jobs, runner_type, instance_key, reason_code).await
}

pub async fn is_live(
    channels: &RunnerChannels,
    runner_type: &RunnerTypeKey,
    instance_key: &InstanceKey,
) -> Result<bool, PortError> {
    let key = runner_presence_key(
        &SubjectSegment::runner_type(runner_type.as_str()).map_err(transport_error)?,
        &SubjectSegment::instance_key(instance_key.as_str()).map_err(transport_error)?,
    );
    Ok(channels
        .presence_bucket()
        .get(key)
        .await
        .map_err(transport_error)?
        .is_some())
}
