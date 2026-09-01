use std::sync::Arc;

use async_nats::jetstream::AckKind;
use async_nats::jetstream::consumer::{AckPolicy, PullConsumer, pull};
use async_nats::jetstream::stream::Stream;
use bc_jobs::domain::keys::RunnerTypeKey;
use contract_jobs::runner as wire;
use contract_jobs::runner_transport::RunnerStatusFact;
use futures::StreamExt;

use super::observability::Frame;
use super::{RunnerChannels, observability};
use crate::app::{Jobs, logs, run_facts};
use crate::config::ConsumerTuning;
use crate::error::ServiceError;
use crate::supervision::Established;

const STATUS_DURABLE: &str = "svc_jobs_status";
const LOG_DURABLE: &str = "svc_jobs_logs";

pub async fn consume_status(
    channels: RunnerChannels,
    jobs: Arc<Jobs>,
    established: Established,
) -> Result<(), ServiceError> {
    let stream = bind(channels.context(), wire::STATUS_STREAM).await?;
    let consumer = durable(
        &stream,
        STATUS_DURABLE,
        wire::STATUS_FILTER,
        channels.consumer_tuning(),
    )
    .await?;
    established.signal();
    run(consumer, move |subject, payload| {
        let jobs = Arc::clone(&jobs);
        async move { handle_status(&jobs, &subject, &payload).await }
    })
    .await
}

pub async fn consume_logs(
    channels: RunnerChannels,
    jobs: Arc<Jobs>,
    established: Established,
) -> Result<(), ServiceError> {
    let stream = bind(channels.context(), wire::LOG_STREAM).await?;
    let consumer = durable(
        &stream,
        LOG_DURABLE,
        wire::LOG_FILTER,
        channels.consumer_tuning(),
    )
    .await?;
    established.signal();
    run(consumer, move |_subject, payload| {
        let jobs = Arc::clone(&jobs);
        async move { (Frame::Log, handle_log(&jobs, &payload).await) }
    })
    .await
}

/// Resolves what the frame is, then applies it. The resolved identity travels
/// out with the outcome so a drop is counted as the very fact that was
/// dispatched — reading the subject twice is how the two come to disagree.
async fn handle_status(
    jobs: &Jobs,
    subject: &str,
    payload: &[u8],
) -> (Frame, Result<(), ServiceError>) {
    let Some((runner_type, fact)) = status_subject(subject) else {
        tracing::debug!(
            subject,
            "a status subject naming no runner type, acknowledged"
        );
        return (Frame::Unrecognized, Ok(()));
    };
    let Some(fact) = fact else {
        tracing::debug!(subject, "unknown runner status fact, acknowledged");
        return (Frame::Unrecognized, Ok(()));
    };
    (
        Frame::Status(fact),
        apply_status(jobs, runner_type, fact, payload).await,
    )
}

/// The runner type and the fact a status subject names, read once. Extra
/// segments beyond the fact are ignored — the durable's `jobs.status.>` filter
/// accepts them, so what matters is that this reading is the only one.
fn status_subject(subject: &str) -> Option<(&str, Option<RunnerStatusFact>)> {
    let mut segments = subject.split('.').skip(2);
    let runner_type = segments.next()?;
    let fact = segments.next()?;
    Some((runner_type, RunnerStatusFact::parse(fact)))
}

async fn apply_status(
    jobs: &Jobs,
    runner_type: &str,
    fact: RunnerStatusFact,
    payload: &[u8],
) -> Result<(), ServiceError> {
    let runner_type = RunnerTypeKey::new(runner_type)?;
    match fact {
        RunnerStatusFact::Started => {
            run_facts::started(jobs, &runner_type, &serde_json::from_slice(payload)?).await
        }
        RunnerStatusFact::PlanDeclared => {
            run_facts::plan_declared(jobs, &serde_json::from_slice(payload)?).await
        }
        RunnerStatusFact::StepStarted => {
            run_facts::step_started(jobs, &serde_json::from_slice(payload)?).await
        }
        RunnerStatusFact::Completed => {
            run_facts::completed(jobs, &serde_json::from_slice(payload)?).await
        }
        RunnerStatusFact::Failed => {
            run_facts::failed(jobs, &serde_json::from_slice(payload)?).await
        }
    }
}

async fn handle_log(jobs: &Jobs, payload: &[u8]) -> Result<(), ServiceError> {
    let line: wire::LogLine = serde_json::from_slice(payload)?;
    logs::append(jobs, &line).await
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
    tuning: ConsumerTuning,
) -> Result<PullConsumer, ServiceError> {
    stream
        .create_consumer(pull::Config {
            durable_name: Some(durable_name.to_owned()),
            filter_subject: filter.to_owned(),
            ack_policy: AckPolicy::Explicit,
            ack_wait: tuning.ack_wait,
            max_ack_pending: tuning.max_ack_pending,
            max_deliver: tuning.max_deliver,
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
    F: std::future::Future<Output = (Frame, Result<(), ServiceError>)> + Send,
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
        let (frame, outcome) = handle(subject.clone(), payload).await;
        let kind = settle(&subject, frame, outcome);
        if let Err(error) = message.ack_with(kind).await {
            tracing::warn!(error = %error, "settling a runner fact failed");
        }
    }
    Ok(())
}

fn settle(subject: &str, frame: Frame, outcome: Result<(), ServiceError>) -> AckKind {
    match outcome {
        Ok(()) => AckKind::Ack,
        Err(ServiceError::Infra(detail)) => {
            tracing::error!(
                subject = %subject,
                detail,
                "a runner fact failed on infrastructure; it will be redelivered"
            );
            AckKind::Nak(None)
        }
        Err(ServiceError::Contended) => {
            tracing::debug!(subject = %subject, "a runner fact lost a write race, redelivered");
            AckKind::Nak(None)
        }
        Err(permanent) => {
            observability::discarded(frame);
            tracing::warn!(
                subject = %subject,
                error = %permanent,
                "a runner fact was refused permanently, discarded"
            );
            AckKind::Term
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_status_subject_names_its_runner_type_and_its_fact() {
        // Given: the subject a runner publishes a terminal fact on
        // When/Then: both halves are read, and the fact is the typed one the metric will name
        assert_eq!(
            status_subject("jobs.status.analyst.failed"),
            Some(("analyst", Some(RunnerStatusFact::Failed))),
        );
    }

    #[test]
    fn a_subject_carrying_extra_segments_is_the_same_fact_to_the_dispatch_and_to_the_metric() {
        // Given: a subject deeper than four segments — the durable filter `jobs.status.>` takes
        // it, so a runner can publish one whether or not anyone planned for it
        let deep = "jobs.status.analyst.failed.v2";
        // When: the subject is read
        let (runner_type, fact) = status_subject(deep).expect("the subject names a runner type");
        // Then: it is handled as the terminal fact it is — and because this reading is the only
        // one, the drop of such a frame is counted as `failed`, never off in another series
        assert_eq!(runner_type, "analyst");
        assert_eq!(fact, Some(RunnerStatusFact::Failed));
        assert_eq!(
            Frame::Status(fact.expect("a fact")),
            Frame::Status(RunnerStatusFact::Failed),
        );
    }

    #[test]
    fn a_subject_that_names_no_fact_this_service_knows_is_read_as_none() {
        // Given: a subject with an unknown fact segment, and one that stops short
        // When/Then: the first names no fact, the second names no runner type either — both are
        // acknowledged and left uncounted as a status fact
        assert_eq!(
            status_subject("jobs.status.analyst.invented"),
            Some(("analyst", None)),
        );
        assert_eq!(status_subject("jobs.status.analyst"), None);
        assert_eq!(status_subject("jobs.status"), None);
    }
}
