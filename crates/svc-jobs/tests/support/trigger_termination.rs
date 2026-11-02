use std::time::Duration;

use async_nats::jetstream::AckKind;
use async_nats::jetstream::consumer::pull;
use futures::StreamExt;
use serde_json::Value;
use uuid::Uuid;

use super::runner::FakeRunner;
use super::wire;

pub struct TriggerConsumer {
    messages: pull::Stream,
}

pub struct TakenTrigger {
    message: async_nats::jetstream::Message,
    pub payload: Value,
}

impl FakeRunner<'_> {
    pub async fn consume_triggers(&self) -> TriggerConsumer {
        let stream = self
            .nats
            .jetstream()
            .get_stream(wire::TRIGGER_STREAM)
            .await
            .expect("the trigger stream is declared before the service boots");
        let consumer = stream
            .create_consumer(pull::Config {
                name: Some(format!("runner-{}", Uuid::now_v7().simple())),
                filter_subject: wire::trigger_subject(&self.runner_type),
                ack_policy: async_nats::jetstream::consumer::AckPolicy::Explicit,
                ..pull::Config::default()
            })
            .await
            .expect("a runner binds a consumer on the trigger stream");
        TriggerConsumer {
            messages: consumer
                .messages()
                .await
                .expect("the consumer delivers its messages"),
        }
    }

    pub async fn publish_trigger_the_service_never_sent(&self) {
        self.nats
            .jetstream()
            .publish(wire::trigger_subject(&self.runner_type), "{}".into())
            .await
            .expect("the foreign trigger is published")
            .await
            .expect("the foreign trigger is acknowledged");
    }
}

impl TriggerConsumer {
    pub async fn next(&mut self, timeout: Duration) -> TakenTrigger {
        let message = tokio::time::timeout(timeout, self.messages.next())
            .await
            .unwrap_or_else(|_| panic!("no trigger was delivered within {timeout:?}"))
            .expect("the trigger consumer stays open")
            .expect("a trigger is delivered");
        let payload = serde_json::from_slice(&message.payload).unwrap_or(Value::Null);
        TakenTrigger { message, payload }
    }
}

impl TakenTrigger {
    pub async fn terminate(self) {
        self.message
            .ack_with(AckKind::Term)
            .await
            .expect("the termination is acknowledged by the broker");
    }

    pub async fn acknowledge(self) {
        self.message
            .ack()
            .await
            .expect("the acknowledgement reaches the broker");
    }
}
