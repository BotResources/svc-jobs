use serde_json::{Map, Value, json};
use uuid::Uuid;

use super::Producer;
use crate::support::wire;

impl Producer<'_> {
    pub async fn cancel(&self, job_id: Uuid) {
        self.cancel_as(Uuid::now_v7(), job_id).await;
    }

    pub async fn cancel_v2(&self, job_id: Uuid) {
        self.cancel_v2_as(Uuid::now_v7(), job_id).await;
    }

    pub async fn cancel_as(&self, command_id: Uuid, job_id: Uuid) {
        self.send_resolution_at_as(
            command_id,
            wire::VERB_CANCEL,
            wire::CONTRACT_V1,
            resolution_payload(job_id),
        )
        .await;
    }

    pub async fn cancel_v2_as(&self, command_id: Uuid, job_id: Uuid) {
        self.send_resolution_at_as(
            command_id,
            wire::VERB_CANCEL,
            wire::CONTRACT_V2,
            resolution_payload(job_id),
        )
        .await;
    }

    pub async fn finish(&self, job_id: Uuid) {
        self.finish_as(Uuid::now_v7(), job_id).await;
    }

    pub async fn finish_v2(&self, job_id: Uuid) {
        self.finish_v2_as(Uuid::now_v7(), job_id).await;
    }

    pub async fn finish_as(&self, command_id: Uuid, job_id: Uuid) {
        self.send_resolution_at_as(
            command_id,
            wire::VERB_FINISH,
            wire::CONTRACT_V1,
            resolution_payload(job_id),
        )
        .await;
    }

    pub async fn finish_v2_as(&self, command_id: Uuid, job_id: Uuid) {
        self.send_resolution_at_as(
            command_id,
            wire::VERB_FINISH,
            wire::CONTRACT_V2,
            resolution_payload(job_id),
        )
        .await;
    }

    pub async fn fail(&self, job_id: Uuid, note: Option<&str>) {
        self.fail_as(Uuid::now_v7(), job_id, note).await;
    }

    pub async fn fail_v2(&self, job_id: Uuid, note: Option<&str>) {
        self.fail_v2_as(Uuid::now_v7(), job_id, note).await;
    }

    pub async fn fail_as(&self, command_id: Uuid, job_id: Uuid, note: Option<&str>) {
        self.send_resolution_at_as(
            command_id,
            wire::VERB_FAIL,
            wire::CONTRACT_V1,
            failure_payload(job_id, note),
        )
        .await;
    }

    pub async fn fail_v2_as(&self, command_id: Uuid, job_id: Uuid, note: Option<&str>) {
        self.send_resolution_at_as(
            command_id,
            wire::VERB_FAIL,
            wire::CONTRACT_V2,
            failure_payload(job_id, note),
        )
        .await;
    }

    async fn send_resolution_at_as(
        &self,
        command_id: Uuid,
        verb: &str,
        version: u8,
        payload: Value,
    ) {
        let command = wire::command_envelope_at(
            command_id,
            verb,
            version,
            Uuid::now_v7(),
            self.account_id,
            payload,
        );
        self.send_at(verb, version, &command).await;
    }
}

fn resolution_payload(job_id: Uuid) -> Value {
    json!({ "job_id": job_id.to_string() })
}

fn failure_payload(job_id: Uuid, note: Option<&str>) -> Value {
    let mut payload = Map::new();
    payload.insert("job_id".to_owned(), json!(job_id.to_string()));
    if let Some(note) = note {
        payload.insert("note".to_owned(), json!(note));
    }
    Value::Object(payload)
}
