use std::sync::Arc;
use std::time::Duration;

use bc_jobs::ports::transport::TriggerSequence;
use contract_jobs::runner as wire;
use futures::StreamExt;
use serde::Deserialize;

use super::RunnerChannels;
use crate::app::trigger_outcomes::{self, Outcome};
use crate::app::{Jobs, terminated_trigger};
use crate::error::ServiceError;
use crate::supervision::Established;

pub const TERMINATION_QUEUE_GROUP: &str = "svc_jobs_trigger_terminated";

const RESUBSCRIBE_PAUSE: Duration = Duration::from_secs(5);

#[derive(Deserialize)]
struct Terminated {
    stream: String,
    stream_seq: u64,
}

pub fn advisory_filter() -> String {
    format!(
        "$JS.EVENT.ADVISORY.CONSUMER.MSG_TERMINATED.{}.>",
        wire::TRIGGER_STREAM
    )
}

pub fn terminated_sequence(payload: &[u8]) -> Option<TriggerSequence> {
    let advisory: Terminated = serde_json::from_slice(payload).ok()?;
    (advisory.stream == wire::TRIGGER_STREAM)
        .then(|| TriggerSequence::new(advisory.stream_seq))
        .flatten()
}

pub async fn listen(
    channels: RunnerChannels,
    jobs: Arc<Jobs>,
    established: Established,
) -> Result<(), ServiceError> {
    established.signal();
    let client = channels.client();
    loop {
        match subscribe(&client).await {
            Ok(advisories) => serve(advisories, &jobs).await,
            Err(error) => {
                tracing::warn!(error = %error, "the trigger termination advisory could not be subscribed");
            }
        }
        tokio::time::sleep(RESUBSCRIBE_PAUSE).await;
    }
}

async fn subscribe(client: &async_nats::Client) -> Result<async_nats::Subscriber, ServiceError> {
    let advisories = client
        .queue_subscribe(advisory_filter(), TERMINATION_QUEUE_GROUP.to_owned())
        .await
        .map_err(|error| ServiceError::Infra(error.to_string()))?;
    client
        .flush()
        .await
        .map_err(|error| ServiceError::Infra(error.to_string()))?;
    Ok(advisories)
}

async fn serve(mut advisories: async_nats::Subscriber, jobs: &Arc<Jobs>) {
    while let Some(message) = advisories.next().await {
        let Some(sequence) = terminated_sequence(&message.payload) else {
            trigger_outcomes::record(Outcome::Unreadable);
            tracing::warn!(
                subject = %message.subject,
                "a trigger termination advisory this service cannot read was ignored"
            );
            continue;
        };
        let jobs = Arc::clone(jobs);
        tokio::spawn(async move {
            let outcome = match terminated_trigger::terminated(&jobs, sequence).await {
                Ok(outcome) => outcome,
                Err(error) => {
                    tracing::warn!(
                        sequence = sequence.get(),
                        error = %error,
                        "failing the run of a terminated trigger did not complete; the run stays \
                         pending because the advisory is delivered at most once"
                    );
                    Outcome::Unsettled
                }
            };
            trigger_outcomes::record(outcome);
        });
    }
    tracing::warn!("the trigger termination subscription ended; it is subscribed again");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_advisory_filter_names_the_trigger_stream_and_any_consumer() {
        assert_eq!(
            advisory_filter(),
            "$JS.EVENT.ADVISORY.CONSUMER.MSG_TERMINATED.JOBS_TRIGGER.>"
        );
    }

    #[test]
    fn the_stream_sequence_is_read_from_the_advisory_of_the_trigger_stream() {
        let payload = br#"{"type":"io.nats.jetstream.advisory.v1.terminated","id":"x","stream":"JOBS_TRIGGER","consumer":"c","consumer_seq":3,"stream_seq":42,"deliveries":1}"#;
        assert_eq!(
            terminated_sequence(payload).map(TriggerSequence::get),
            Some(42)
        );
    }

    #[test]
    fn an_advisory_of_another_stream_or_without_a_sequence_names_no_run() {
        let other = br#"{"stream":"JOBS_STATUS","stream_seq":42}"#;
        let zero = br#"{"stream":"JOBS_TRIGGER","stream_seq":0}"#;
        assert_eq!(terminated_sequence(other), None);
        assert_eq!(terminated_sequence(zero), None);
        assert_eq!(terminated_sequence(b"not json"), None);
        assert_eq!(terminated_sequence(br#"{"stream":"JOBS_TRIGGER"}"#), None);
    }
}
