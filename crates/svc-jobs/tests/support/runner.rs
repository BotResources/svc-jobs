use br_test_harness::TestNats;
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use tokio::task::JoinHandle;
use uuid::Uuid;

use super::db::Durable;
use super::fixture::JobsFixture;
use super::{clock, wire};

pub struct FakeRunner<'a> {
    pub(super) nats: &'a TestNats,
    pub runner_type: String,
    pub instance_key: String,
    pub version: String,
    pub(super) status: Arc<Mutex<String>>,
    pub(super) capacity: Arc<Mutex<u32>>,
    pub(super) heartbeat: Option<JoinHandle<()>>,
    pub(super) cursor: u64,
}

impl<'a> FakeRunner<'a> {
    pub fn new(nats: &'a TestNats, runner_type: &str, instance_key: &str) -> Self {
        Self {
            nats,
            runner_type: runner_type.to_string(),
            instance_key: instance_key.to_string(),
            version: "0.1.0".to_string(),
            status: Arc::new(Mutex::new(wire::STATUS_READY.to_string())),
            capacity: Arc::new(Mutex::new(1)),
            heartbeat: None,
            cursor: 1,
        }
    }

    pub fn declaring_capacity(self, capacity: u32) -> Self {
        self.set_capacity(capacity);
        self
    }

    pub async fn announce_capacity(&self, capacity: u32) {
        self.set_capacity(capacity);
        self.rewrite_presence().await;
    }

    pub async fn connect(&mut self) {
        self.connect_as(wire::STATUS_READY).await;
    }

    pub async fn connect_as(&mut self, status: &str) {
        self.announce(status).await;
        self.start_refreshing().await;
    }

    pub async fn announce(&self, status: &str) {
        self.set_status(status);
        self.rewrite_presence().await;
    }

    async fn rewrite_presence(&self) {
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

    pub fn resume_after(&mut self, claimed_by: &FakeRunner) {
        self.cursor = claimed_by.cursor;
    }

    pub async fn start_run(&self, trigger: &Value) {
        self.publish_status(
            wire::FACT_STARTED,
            json!({
                "run_id": run_id(trigger).to_string(),
                "job_id": job_id(trigger).to_string(),
                "instance_key": self.instance_key,
                "started_at": clock::now_rfc3339(),
            }),
        )
        .await;
    }

    pub async fn start_applied(&self, fixture: &JobsFixture, trigger: &Value) {
        self.start_run(trigger).await;
        let durable = Durable::open(&fixture.app_url()).await;
        durable
            .await_count(
                "SELECT count(*) FROM run_starts WHERE run_id = $1",
                run_id(trigger),
                1,
                "the start of the run",
            )
            .await;
        durable.close().await;
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
                "started_at": clock::now_rfc3339(),
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
                "occurred_at": clock::now_rfc3339(),
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
                "occurred_at": clock::now_rfc3339(),
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
        self.log_line_at(trigger, step_index, level, message, clock::now_rfc3339())
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
