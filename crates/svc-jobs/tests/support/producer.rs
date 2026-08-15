use br_core_integration::IntegrationCommand;
use br_test_harness::FabricTestNats;
use serde_json::{Value, json};
use uuid::Uuid;

use super::wire;

pub struct JobDeclaration {
    pub job_id: Uuid,
    pub runner_type: String,
    pub config: Option<Value>,
    pub parent_job_id: Option<Uuid>,
    pub triggered_by: Option<(Uuid, String)>,
    pub source: Option<(String, Uuid)>,
    pub max_attempts: Option<i64>,
}

impl JobDeclaration {
    pub fn new(runner_type: &str) -> Self {
        Self {
            job_id: Uuid::now_v7(),
            runner_type: runner_type.to_string(),
            config: None,
            parent_job_id: None,
            triggered_by: None,
            source: None,
            max_attempts: None,
        }
    }

    pub fn with_id(mut self, job_id: Uuid) -> Self {
        self.job_id = job_id;
        self
    }

    pub fn with_config(mut self, config: Value) -> Self {
        self.config = Some(config);
        self
    }

    pub fn with_parent(mut self, parent_job_id: Uuid) -> Self {
        self.parent_job_id = Some(parent_job_id);
        self
    }

    pub fn triggered_by(mut self, user_id: Uuid, display_name: &str) -> Self {
        self.triggered_by = Some((user_id, display_name.to_string()));
        self
    }

    pub fn with_source(mut self, bc: &str, entity_id: Uuid) -> Self {
        self.source = Some((bc.to_string(), entity_id));
        self
    }

    pub fn with_max_attempts(mut self, max_attempts: i64) -> Self {
        self.max_attempts = Some(max_attempts);
        self
    }

    pub fn payload(&self, producer: &str) -> Value {
        json!({
            "job_id": self.job_id.to_string(),
            "runner_type": self.runner_type,
            "producer": producer,
            "config": self.config,
            "parent_job_id": self.parent_job_id.map(|id| id.to_string()),
            "triggered_by": self.triggered_by.as_ref().map(|(id, name)| json!({
                "id": id.to_string(),
                "display_name": name,
            })),
            "source_bc": self.source.as_ref().map(|(bc, _)| bc.clone()),
            "source_entity_id": self.source.as_ref().map(|(_, entity_id)| entity_id.to_string()),
            "max_attempts": self.max_attempts,
        })
    }
}

pub struct Producer<'a> {
    fabric: &'a FabricTestNats,
    pub bc: String,
    pub account_id: Uuid,
}

impl<'a> Producer<'a> {
    pub fn new(fabric: &'a FabricTestNats, bc: &str) -> Self {
        Self {
            fabric,
            bc: bc.to_string(),
            account_id: Uuid::now_v7(),
        }
    }

    pub fn create_command(&self, declaration: &JobDeclaration) -> IntegrationCommand<Value> {
        wire::command_envelope(
            Uuid::now_v7(),
            wire::VERB_CREATE,
            Uuid::now_v7(),
            self.account_id,
            declaration.payload(&self.bc),
        )
    }

    pub async fn send(&self, verb: &str, command: &IntegrationCommand<Value>) {
        self.fabric
            .fabric()
            .publish_command(&wire::command_coords(verb), command)
            .await
            .unwrap_or_else(|error| panic!("publishing jobs.job.{verb} onto the fabric: {error}"));
    }

    pub async fn declare(&self, declaration: &JobDeclaration) -> IntegrationCommand<Value> {
        let command = self.create_command(declaration);
        self.send(wire::VERB_CREATE, &command).await;
        command
    }

    pub async fn redeliver(&self, command: &IntegrationCommand<Value>) {
        self.send(wire::VERB_CREATE, command).await;
    }

    pub async fn reuse_command_id_with(
        &self,
        command: &IntegrationCommand<Value>,
        payload: Value,
    ) -> IntegrationCommand<Value> {
        let mut conflicting = command.clone();
        conflicting.payload = payload;
        self.send(wire::VERB_CREATE, &conflicting).await;
        conflicting
    }

    pub async fn cancel(&self, job_id: Uuid, resolution_id: Uuid) {
        self.send_resolution(wire::VERB_CANCEL, job_id, resolution_id)
            .await;
    }

    pub async fn finish(&self, job_id: Uuid, resolution_id: Uuid) {
        self.send_resolution(wire::VERB_FINISH, job_id, resolution_id)
            .await;
    }

    pub async fn fail(&self, job_id: Uuid, resolution_id: Uuid) {
        self.send_resolution(wire::VERB_FAIL, job_id, resolution_id)
            .await;
    }

    async fn send_resolution(&self, verb: &str, job_id: Uuid, resolution_id: Uuid) {
        let command = wire::command_envelope(
            Uuid::now_v7(),
            verb,
            Uuid::now_v7(),
            self.account_id,
            json!({ "id": resolution_id.to_string(), "job_id": job_id.to_string() }),
        );
        self.send(verb, &command).await;
    }
}
