use std::sync::Arc;

use contract_jobs::runner_surface as wire;
use futures::StreamExt;
use serde::Serialize;

use super::RunnerChannels;
use crate::app::{Jobs, directory};
use crate::error::ServiceError;
use crate::supervision::Established;

pub const SURFACE_QUEUE_GROUP: &str = "svc_jobs_runner_surface";

pub async fn respond(
    channels: RunnerChannels,
    jobs: Arc<Jobs>,
    established: Established,
) -> Result<(), ServiceError> {
    let client = channels.client();
    let mut requests = futures::stream::select_all(vec![
        subscribe(&client, wire::REGISTER_AGENTS_FILTER).await?,
        subscribe(&client, wire::LIST_AGENTS_FILTER).await?,
    ]);
    established.signal();
    while let Some(message) = requests.next().await {
        let Some(reply_to) = message.reply.clone() else {
            tracing::debug!(subject = %message.subject, "a surface request with no reply subject");
            continue;
        };
        let client = client.clone();
        let jobs = Arc::clone(&jobs);
        tokio::spawn(async move {
            let bytes = answer(&jobs, message.subject.as_str(), &message.payload).await;
            if let Err(error) = client.publish(reply_to, bytes.into()).await {
                tracing::warn!(error = %error, "a surface reply could not be sent");
            }
        });
    }
    Err(ServiceError::Infra(
        "the runner surface subscription ended; the responder restarts and resubscribes".to_owned(),
    ))
}

async fn subscribe(
    client: &async_nats::Client,
    filter: &str,
) -> Result<async_nats::Subscriber, ServiceError> {
    client
        .queue_subscribe(filter.to_owned(), SURFACE_QUEUE_GROUP.to_owned())
        .await
        .map_err(|error| ServiceError::Infra(error.to_string()))
}

async fn answer(jobs: &Jobs, subject: &str, payload: &[u8]) -> Vec<u8> {
    if let Some(runner_type) = subject.strip_prefix(wire::REGISTER_AGENTS_PREFIX) {
        let reply = match decode::<wire::RegisterAgents>(payload) {
            Some(request) => directory::register_agents(jobs, runner_type, request).await,
            None => malformed(),
        };
        return encode(&reply);
    }
    if let Some(runner_type) = subject.strip_prefix(wire::LIST_AGENTS_PREFIX) {
        let reply = match decode::<wire::ListAgents>(payload) {
            Some(request) => directory::list_agents(jobs, runner_type, request).await,
            None => wire::ListAgentsReply::refused(wire::REASON_MALFORMED, MALFORMED_WORDS),
        };
        return encode(&reply);
    }
    encode(&malformed())
}

const MALFORMED_WORDS: &str = "Jobs cannot read the request.";

fn malformed() -> wire::SurfaceReply {
    wire::SurfaceReply::refused(wire::REASON_MALFORMED, MALFORMED_WORDS)
}

fn decode<T: serde::de::DeserializeOwned>(payload: &[u8]) -> Option<T> {
    serde_json::from_slice(payload).ok()
}

fn encode<T: Serialize>(reply: &T) -> Vec<u8> {
    serde_json::to_vec(reply).unwrap_or_default()
}
