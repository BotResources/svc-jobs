use std::time::Duration;

use br_test_harness::{EventCapture, FabricTestNats, wait_until};
use serde_json::Value;
use uuid::Uuid;

use super::wire;

pub struct EventLog {
    capture: EventCapture,
}

pub struct ObservedEvent {
    pub fact: String,
    pub envelope: Value,
}

impl ObservedEvent {
    pub fn payload(&self) -> Value {
        self.envelope["payload"].clone()
    }

    pub fn correlation_id(&self) -> String {
        self.envelope["metadata"]["correlation_id"]
            .as_str()
            .unwrap_or_default()
            .to_string()
    }
}

impl EventLog {
    pub async fn open(fabric: &FabricTestNats) -> Self {
        let coords = wire::all_event_coords();
        let refs: Vec<_> = coords.iter().collect();
        Self {
            capture: fabric.capture_events(&refs).await,
        }
    }

    pub fn all(&self) -> Vec<ObservedEvent> {
        let mut observed = Vec::new();
        for correlation in dedup(self.capture.correlation_ids()) {
            for message in self.capture.for_correlation(correlation) {
                let envelope: Value = serde_json::from_slice(&message.payload)
                    .unwrap_or_else(|e| panic!("an integration event must be JSON: {e}"));
                observed.push(ObservedEvent {
                    fact: wire::fact_of(&message.subject),
                    envelope,
                });
            }
        }
        observed
    }

    pub fn of(&self, fact: &str, job_id: Uuid) -> Vec<ObservedEvent> {
        let wanted = job_id.to_string();
        self.all()
            .into_iter()
            .filter(|event| {
                event.fact == fact && event.envelope["payload"]["job_id"] == wanted.as_str()
            })
            .collect()
    }

    pub fn count_of(&self, fact: &str, job_id: Uuid) -> usize {
        self.of(fact, job_id).len()
    }

    pub async fn expect_one(&self, fact: &str, job_id: Uuid, timeout: Duration) -> ObservedEvent {
        let log = self;
        let arrived = wait_until(timeout, || async move { log.count_of(fact, job_id) >= 1 }).await;
        assert!(
            arrived,
            "no integration.evt.jobs.job.{fact}.v1 for job {job_id} within {timeout:?}"
        );
        let mut events = self.of(fact, job_id);
        assert_eq!(
            events.len(),
            1,
            "exactly one {fact} event is owed for job {job_id}, got {}",
            events.len()
        );
        events.remove(0)
    }

    pub async fn expect_exactly(
        &self,
        fact: &str,
        job_id: Uuid,
        expected: usize,
        timeout: Duration,
    ) -> Vec<ObservedEvent> {
        let log = self;
        let arrived = wait_until(
            timeout,
            || async move { log.count_of(fact, job_id) >= expected },
        )
        .await;
        assert!(
            arrived,
            "expected {expected} {fact} events for job {job_id} within {timeout:?}, got {}",
            self.count_of(fact, job_id)
        );
        let events = self.of(fact, job_id);
        assert_eq!(
            events.len(),
            expected,
            "expected exactly {expected} {fact} events for job {job_id}"
        );
        events
    }

    pub async fn expect_none(&self, fact: &str, job_id: Uuid, quiet: Duration) {
        tokio::time::sleep(quiet).await;
        let observed = self.count_of(fact, job_id);
        assert_eq!(
            observed, 0,
            "no {fact} event may be published for job {job_id}, but {observed} arrived"
        );
    }

    pub async fn stop(self) {
        self.capture.stop().await;
    }
}

fn dedup(mut ids: Vec<Uuid>) -> Vec<Uuid> {
    ids.sort();
    ids.dedup();
    ids
}
