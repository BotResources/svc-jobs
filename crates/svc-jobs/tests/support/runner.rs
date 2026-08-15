use std::sync::{Arc, Mutex};
use std::time::Duration;

use br_test_harness::{TestNats, wait_until};
use chrono::Utc;
use serde_json::{Value, json};
use tokio::task::JoinHandle;
use uuid::Uuid;

use super::{infra, wire};

pub struct FakeRunner<'a> {
    nats: &'a TestNats,
    pub runner_type: String,
    pub instance_key: String,
    pub version: String,
    status: Arc<Mutex<String>>,
    heartbeat: Option<JoinHandle<()>>,
    cursor: u64,
}

impl<'a> FakeRunner<'a> {
    pub fn new(nats: &'a TestNats, runner_type: &str, instance_key: &str) -> Self {
        Self {
            nats,
            runner_type: runner_type.to_string(),
            instance_key: instance_key.to_string(),
            version: "0.1.0".to_string(),
            status: Arc::new(Mutex::new("IDLE".to_string())),
            heartbeat: None,
            cursor: 1,
        }
    }

    pub async fn connect(&mut self) {
        self.announce("IDLE").await;
        self.start_refreshing().await;
    }

    pub async fn announce(&self, status: &str) {
        self.set_status(status);
        let store = self
            .nats
            .jetstream()
            .get_key_value(wire::PRESENCE_BUCKET)
            .await
            .expect("the presence bucket is declared before the service boots");
        store
            .put(self.presence_key(), self.presence_value().into())
            .await
            .expect("writing the presence entry");
    }

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

    async fn start_refreshing(&mut self) {
        self.stop_refreshing();
        let key = self.presence_key();
        let status = Arc::clone(&self.status);
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
                let value = presence_value(&runner_type, &instance_key, &version, &current);
                if store.put(key.clone(), value.into()).await.is_err() {
                    return;
                }
            }
        }));
    }

    fn stop_refreshing(&mut self) {
        if let Some(handle) = self.heartbeat.take() {
            handle.abort();
        }
    }

    fn set_status(&self, status: &str) {
        *self
            .status
            .lock()
            .expect("the announced status is writable") = status.to_string();
    }

    fn presence_key(&self) -> String {
        wire::presence_key(&self.runner_type, &self.instance_key)
    }

    fn presence_value(&self) -> Vec<u8> {
        presence_value(
            &self.runner_type,
            &self.instance_key,
            &self.version,
            &current_status(&self.status),
        )
    }

    pub fn resume_after(&mut self, claimed_by: &FakeRunner) {
        self.cursor = claimed_by.cursor;
    }

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

    pub async fn start_run(&self, trigger: &Value) {
        self.publish_status(
            wire::FACT_STARTED,
            json!({
                "run_id": run_id(trigger).to_string(),
                "job_id": job_id(trigger).to_string(),
                "instance_key": self.instance_key,
                "started_at": Utc::now().to_rfc3339(),
            }),
        )
        .await;
    }

    pub async fn declare_plan(&self, trigger: &Value, labels: &[&str]) {
        self.publish_status(
            wire::FACT_PLAN_DECLARED,
            json!({
                "run_id": run_id(trigger).to_string(),
                "job_id": job_id(trigger).to_string(),
                "steps": labels,
            }),
        )
        .await;
    }

    pub async fn start_step(&self, trigger: &Value, index: i64, label: &str) {
        self.publish_status(
            wire::FACT_STEP_STARTED,
            json!({
                "run_id": run_id(trigger).to_string(),
                "job_id": job_id(trigger).to_string(),
                "index": index,
                "label": label,
                "started_at": Utc::now().to_rfc3339(),
            }),
        )
        .await;
    }

    pub async fn complete_run(&self, trigger: &Value) {
        self.publish_status(
            wire::FACT_COMPLETED,
            json!({
                "run_id": run_id(trigger).to_string(),
                "job_id": job_id(trigger).to_string(),
                "occurred_at": Utc::now().to_rfc3339(),
            }),
        )
        .await;
    }

    pub async fn fail_run(
        &self,
        trigger: &Value,
        kind: &str,
        reason_code: &str,
        retry_after_seconds: Option<i64>,
    ) {
        self.publish_status(
            wire::FACT_FAILED,
            json!({
                "run_id": run_id(trigger).to_string(),
                "job_id": job_id(trigger).to_string(),
                "occurred_at": Utc::now().to_rfc3339(),
                "report": {
                    "kind": kind,
                    "reason_code": reason_code,
                    "params": { "attempt": trigger["attempt"].clone() },
                    "diagnostic": { "stderr": "runner double diagnostic" },
                },
                "retry_after_seconds": retry_after_seconds,
            }),
        )
        .await;
    }

    pub async fn publish_status(&self, fact: &str, payload: Value) {
        self.nats
            .publish_raw(
                &wire::status_subject(&self.runner_type, fact),
                serde_json::to_vec(&payload).expect("a status fact serializes"),
            )
            .await;
    }

    pub async fn log_line(
        &self,
        trigger: &Value,
        step_index: Option<i64>,
        level: &str,
        message: &str,
    ) {
        self.log_line_at(trigger, step_index, level, message, Utc::now().to_rfc3339())
            .await;
    }

    pub async fn log_line_at(
        &self,
        trigger: &Value,
        step_index: Option<i64>,
        level: &str,
        message: &str,
        logged_at: String,
    ) {
        self.nats
            .publish_raw(
                &wire::log_subject(&self.runner_type),
                serde_json::to_vec(&json!({
                    "run_id": run_id(trigger).to_string(),
                    "job_id": job_id(trigger).to_string(),
                    "step_index": step_index,
                    "level": level,
                    "message": message,
                    "logged_at": logged_at,
                }))
                .expect("a log line serializes"),
            )
            .await;
    }

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

fn current_status(status: &Arc<Mutex<String>>) -> String {
    status
        .lock()
        .expect("the announced status is readable")
        .clone()
}

fn presence_value(runner_type: &str, instance_key: &str, version: &str, status: &str) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "runner_type": runner_type,
        "instance_key": instance_key,
        "runner_version": version,
        "status": status,
        "observed_at": Utc::now().to_rfc3339(),
    }))
    .expect("presence value serializes")
}

pub fn run_id(trigger: &Value) -> Uuid {
    field_uuid(trigger, "run_id")
}

pub fn job_id(trigger: &Value) -> Uuid {
    field_uuid(trigger, "job_id")
}

pub fn attempt_number(trigger: &Value) -> i64 {
    trigger["attempt"]
        .as_i64()
        .unwrap_or_else(|| panic!("a trigger carries its attempt number: {trigger}"))
}

fn field_uuid(value: &Value, field: &str) -> Uuid {
    let raw = value[field]
        .as_str()
        .unwrap_or_else(|| panic!("expected '{field}' on: {value}"));
    Uuid::parse_str(raw).unwrap_or_else(|e| panic!("'{field}' must be a UUID: {raw} ({e})"))
}
