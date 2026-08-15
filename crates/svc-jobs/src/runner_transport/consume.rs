use std::sync::Arc;

use async_nats::jetstream::consumer::{AckPolicy, PullConsumer, pull};
use async_nats::jetstream::stream::Stream;
use bc_jobs::domain::keys::RunnerTypeKey;
use contract_jobs::runner as wire;
use futures::StreamExt;

use super::RunnerChannels;
use crate::app::{Jobs, logs, run_facts};
use crate::error::ServiceError;

const STATUS_DURABLE: &str = "svc_jobs_status";
const LOG_DURABLE: &str = "svc_jobs_logs";

pub async fn consume_status(channels: RunnerChannels, jobs: Arc<Jobs>) -> Result<(), ServiceError> {
    let stream = bind(channels.context(), wire::STATUS_STREAM).await?;
    let consumer = durable(&stream, STATUS_DURABLE, wire::STATUS_FILTER).await?;
    run(consumer, move |subject, payload| {
        let jobs = Arc::clone(&jobs);
        async move { handle_status(&jobs, &subject, &payload).await }
    })
    .await
}

pub async fn consume_logs(channels: RunnerChannels, jobs: Arc<Jobs>) -> Result<(), ServiceError> {
    let stream = bind(channels.context(), wire::LOG_STREAM).await?;
    let consumer = durable(&stream, LOG_DURABLE, wire::LOG_FILTER).await?;
    run(consumer, move |_subject, payload| {
        let jobs = Arc::clone(&jobs);
        async move {
            let line: wire::LogLine = serde_json::from_slice(&payload)?;
            logs::append(&jobs, &line).await
        }
    })
    .await
}

async fn handle_status(jobs: &Jobs, subject: &str, payload: &[u8]) -> Result<(), ServiceError> {
    let mut segments = subject.split('.').skip(2);
    let (Some(runner_type), Some(fact)) = (segments.next(), segments.next()) else {
        return Ok(());
    };
    let runner_type = RunnerTypeKey::new(runner_type)?;
    match fact {
        "started" => {
            run_facts::started(jobs, &runner_type, &serde_json::from_slice(payload)?).await
        }
        "plan_declared" => run_facts::plan_declared(jobs, &serde_json::from_slice(payload)?).await,
        "step_started" => run_facts::step_started(jobs, &serde_json::from_slice(payload)?).await,
        "completed" => run_facts::completed(jobs, &serde_json::from_slice(payload)?).await,
        "failed" => run_facts::failed(jobs, &serde_json::from_slice(payload)?).await,
        other => {
            tracing::debug!(fact = other, "unknown runner status fact, acknowledged");
            Ok(())
        }
    }
}

async fn bind(
    context: &async_nats::jetstream::Context,
    name: &str,
) -> Result<Stream, ServiceError> {
    context.get_stream(name).await.map_err(|error| {
        ServiceError::Infra(format!(
            "the declared runner-transport stream '{name}' is absent: {error}"
        ))
    })
}

async fn durable(
    stream: &Stream,
    durable_name: &str,
    filter: &str,
) -> Result<PullConsumer, ServiceError> {
    stream
        .create_consumer(pull::Config {
            durable_name: Some(durable_name.to_owned()),
            filter_subject: filter.to_owned(),
            ack_policy: AckPolicy::Explicit,
            ..Default::default()
        })
        .await
        .map_err(|error| {
            ServiceError::Infra(format!("binding the durable '{durable_name}': {error}"))
        })
}

async fn run<H, F>(consumer: PullConsumer, handle: H) -> Result<(), ServiceError>
where
    H: Fn(String, Vec<u8>) -> F + Send + Sync,
    F: std::future::Future<Output = Result<(), ServiceError>> + Send,
{
    let messages = consumer
        .messages()
        .await
        .map_err(|error| ServiceError::Infra(error.to_string()))?;
    let mut messages = std::pin::pin!(messages);
    while let Some(message) = messages.next().await {
        let message = match message {
            Ok(message) => message,
            Err(error) => {
                tracing::warn!(error = %error, "the runner transport consumer yielded an error");
                continue;
            }
        };
        let subject = message.subject.as_str().to_owned();
        let payload = message.payload.to_vec();
        match handle(subject.clone(), payload).await {
            Ok(()) => {
                if let Err(error) = message.ack().await {
                    tracing::warn!(error = %error, "acknowledging a runner fact failed");
                }
            }
            Err(error) => {
                tracing::error!(
                    subject = %subject,
                    error = %error,
                    "a runner fact could not be recorded; it will be redelivered"
                );
                if let Err(error) = message
                    .ack_with(async_nats::jetstream::AckKind::Nak(None))
                    .await
                {
                    tracing::warn!(error = %error, "negative-acknowledging a runner fact failed");
                }
            }
        }
    }
    Ok(())
}
