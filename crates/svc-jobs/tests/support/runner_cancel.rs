use std::time::Duration;

use br_test_harness::wait_until;
use serde_json::Value;
use uuid::Uuid;

use super::runner::FakeRunner;
use super::wire;

impl FakeRunner<'_> {
    pub async fn cancel_entry(&self, run: Uuid) -> Option<Value> {
        let store = self
            .nats
            .jetstream()
            .get_key_value(wire::CANCEL_BUCKET)
            .await
            .expect("the cancel bucket is declared before the service boots");
        let entry = store
            .get(wire::cancel_key(run))
            .await
            .expect("reading the cancel bucket");
        entry.map(|bytes| {
            serde_json::from_slice(&bytes).unwrap_or_else(|e| panic!("a cancel entry is JSON: {e}"))
        })
    }

    pub async fn await_cancel_entry(&self, run: Uuid, timeout: Duration) -> Value {
        let watcher = self;
        let arrived = wait_until(timeout, || async move {
            watcher.cancel_entry(run).await.is_some()
        })
        .await;
        assert!(
            arrived,
            "no desired-state cancel entry for run {run} within {timeout:?} — an in-flight run \
             is stopped by its entry in the cancel bucket, replayed to every watching instance"
        );
        self.cancel_entry(run)
            .await
            .expect("the entry observed a moment ago is still readable")
    }

    pub async fn await_no_cancel_entry(&self, run: Uuid, timeout: Duration) {
        let watcher = self;
        let removed = wait_until(timeout, || async move {
            watcher.cancel_entry(run).await.is_none()
        })
        .await;
        assert!(
            removed,
            "the desired-state entry for run {run} must be removed once the run is terminal, \
             within {timeout:?} — every instance replays the whole bucket when its watch \
             (re)connects, so a leftover entry re-issues a stop order for a run that ended long ago"
        );
    }

    pub async fn expect_no_cancel_entry(&self, run: Uuid, quiet: Duration) {
        tokio::time::sleep(quiet).await;
        assert!(
            self.cancel_entry(run).await.is_none(),
            "no cancel entry may be written for run {run}"
        );
    }
}
