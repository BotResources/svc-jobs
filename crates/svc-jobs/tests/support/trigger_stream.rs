use std::time::Duration;

use serde_json::Value;

use super::runner::FakeRunner;
use super::wire;

impl FakeRunner<'_> {
    pub async fn next_trigger(&mut self, timeout: Duration) -> Value {
        self.try_next_trigger(timeout).await.unwrap_or_else(|| {
            panic!(
                "no trigger on {} within {timeout:?}",
                wire::trigger_subject(&self.runner_type)
            )
        })
    }

    pub async fn try_next_trigger(&mut self, timeout: Duration) -> Option<Value> {
        let subject = wire::trigger_subject(&self.runner_type);
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if let Some((sequence, payload)) = self.read_from(&subject, self.cursor).await {
                self.cursor = sequence + 1;
                return Some(payload);
            }
            if tokio::time::Instant::now() >= deadline {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    pub async fn expect_no_trigger(&mut self, quiet: Duration) {
        if let Some(trigger) = self.try_next_trigger(quiet).await {
            panic!(
                "no trigger may be dispatched on {} here, got: {trigger}",
                wire::trigger_subject(&self.runner_type)
            );
        }
    }

    pub async fn trigger_count(&self) -> usize {
        let subject = wire::trigger_subject(&self.runner_type);
        let mut sequence = 1;
        let mut seen = 0;
        while let Some((found, _)) = self.read_from(&subject, sequence).await {
            seen += 1;
            sequence = found + 1;
            if seen > 64 {
                panic!("more than 64 triggers on {subject} — the dispatcher is looping");
            }
        }
        seen
    }

    async fn read_from(&self, subject: &str, from: u64) -> Option<(u64, Value)> {
        let stream = self
            .nats
            .jetstream()
            .get_stream(wire::TRIGGER_STREAM)
            .await
            .expect("the trigger stream is declared before the service boots");
        let message = stream
            .raw_message_builder()
            .next_by_subject(subject)
            .sequence(from)
            .send()
            .await
            .ok()?;
        let payload = serde_json::from_slice(&message.payload)
            .unwrap_or_else(|e| panic!("a trigger must be JSON: {e}"));
        Some((message.sequence, payload))
    }
}
