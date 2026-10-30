use std::sync::{Arc, Mutex};

use serde_json::{Value, json};

use super::runner::FakeRunner;
use super::wire;
use super::{clock, infra};

impl FakeRunner<'_> {
    pub async fn disconnect(&mut self) {
        self.stop_refreshing();
        let store = self
            .nats
            .jetstream()
            .get_key_value(wire::PRESENCE_BUCKET)
            .await
            .expect("the presence bucket is declared before the service boots");
        store
            .delete(self.presence_key())
            .await
            .expect("removing the presence entry");
    }

    pub fn crash(&mut self) {
        self.stop_refreshing();
    }

    pub async fn presence_entry(&self) -> Option<Value> {
        let store = self
            .nats
            .jetstream()
            .get_key_value(wire::PRESENCE_BUCKET)
            .await
            .expect("the presence bucket is declared before the service boots");
        store
            .get(self.presence_key())
            .await
            .expect("reading the presence bucket")
            .map(|bytes| {
                serde_json::from_slice(&bytes)
                    .unwrap_or_else(|e| panic!("a presence entry is JSON: {e}"))
            })
    }

    pub async fn write_unreadable_presence(&self) {
        let store = self
            .nats
            .jetstream()
            .get_key_value(wire::PRESENCE_BUCKET)
            .await
            .expect("the presence bucket is declared before the service boots");
        store
            .put(
                self.presence_key(),
                b"not a presence entry at all".to_vec().into(),
            )
            .await
            .expect("writing the presence entry");
    }

    pub(super) async fn start_refreshing(&mut self) {
        self.stop_refreshing();
        let key = self.presence_key();
        let status = Arc::clone(&self.status);
        let capacity = Arc::clone(&self.capacity);
        let runner_type = self.runner_type.clone();
        let instance_key = self.instance_key.clone();
        let version = self.version.clone();
        let store = self
            .nats
            .jetstream()
            .get_key_value(wire::PRESENCE_BUCKET)
            .await
            .expect("the presence bucket is declared before the service boots");
        self.heartbeat = Some(tokio::spawn(async move {
            loop {
                tokio::time::sleep(infra::PRESENCE_REFRESH).await;
                let current = current_status(&status);
                let value = presence_value(
                    &runner_type,
                    &instance_key,
                    &version,
                    &current,
                    current_capacity(&capacity),
                );
                if let Err(error) = store.put(key.clone(), value.into()).await {
                    eprintln!("presence refresh of {key} failed: {error}");
                }
            }
        }));
    }

    fn stop_refreshing(&mut self) {
        if let Some(handle) = self.heartbeat.take() {
            handle.abort();
        }
    }

    pub(super) fn set_status(&self, status: &str) {
        *self
            .status
            .lock()
            .expect("the announced status is writable") = status.to_string();
    }

    pub(super) fn set_capacity(&self, capacity: u32) {
        *self
            .capacity
            .lock()
            .expect("the declared capacity is writable") = capacity;
    }

    pub(super) fn presence_key(&self) -> String {
        wire::presence_key(&self.runner_type, &self.instance_key)
    }

    pub(super) fn presence_value(&self) -> Vec<u8> {
        presence_value(
            &self.runner_type,
            &self.instance_key,
            &self.version,
            &current_status(&self.status),
            current_capacity(&self.capacity),
        )
    }
}

fn current_status(status: &Arc<Mutex<String>>) -> String {
    status
        .lock()
        .expect("the announced status is readable")
        .clone()
}

fn current_capacity(capacity: &Arc<Mutex<u32>>) -> u32 {
    *capacity.lock().expect("the declared capacity is readable")
}

fn presence_value(
    runner_type: &str,
    instance_key: &str,
    version: &str,
    status: &str,
    capacity: u32,
) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "runner_type": runner_type,
        "instance_key": instance_key,
        "runner_version": version,
        "status": status,
        "capacity": capacity,
        "observed_at": clock::now_rfc3339(),
    }))
    .expect("presence value serializes")
}
