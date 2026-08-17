use br_core_integration::IntegrationCommand;
use br_test_harness::FabricTestNats;
use serde_json::{Map, Value, json};
use uuid::Uuid;

use super::wire;

pub enum TriggeringUser {
    Declared(Uuid),
    Named(Uuid, String),
}

impl TriggeringUser {
    pub fn id(&self) -> Uuid {
        match self {
            Self::Declared(id) | Self::Named(id, _) => *id,
        }
    }

    fn wire(&self) -> Value {
        match self {
            Self::Declared(id) => json!(id.to_string()),
            Self::Named(id, display_name) => json!({
                "id": id.to_string(),
                "display_name": display_name,
            }),
        }
    }
}

pub struct JobDeclaration {
    pub job_id: Uuid,
    pub runner_type: String,
    pub config: Option<Value>,
    pub parent_job_id: Option<Uuid>,
    pub triggered_by: Option<TriggeringUser>,
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

    pub fn triggered_by_user(mut self, user_id: Uuid) -> Self {
        self.triggered_by = Some(TriggeringUser::Declared(user_id));
        self
    }

    pub fn triggered_by(mut self, user_id: Uuid, display_name: &str) -> Self {
        self.triggered_by = Some(TriggeringUser::Named(user_id, display_name.to_string()));
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
        let mut fields = Map::new();
        fields.insert("job_id".to_owned(), json!(self.job_id.to_string()));
        fields.insert("runner_type".to_owned(), json!(self.runner_type));
        fields.insert("config".to_owned(), json!(self.config));
        fields.insert(
            "parent_job_id".to_owned(),
            json!(self.parent_job_id.map(|id| id.to_string())),
        );
        fields.insert(
            "triggered_by".to_owned(),
            self.triggered_by
                .as_ref()
                .map_or(Value::Null, TriggeringUser::wire),
        );
        fields.insert("max_attempts".to_owned(), json!(self.max_attempts));
        fields.insert("producer".to_owned(), json!(producer));
        if let Some((bc, entity_id)) = &self.source {
            fields.insert("source_bc".to_owned(), json!(bc));
            fields.insert("source_entity_id".to_owned(), json!(entity_id.to_string()));
        }
        Value::Object(fields)
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

    pub async fn declare_payload(&self, payload: Value) {
        let command = wire::command_envelope(
            Uuid::now_v7(),
            wire::VERB_CREATE,
            Uuid::now_v7(),
            self.account_id,
            payload,
        );
        self.send(wire::VERB_CREATE, &command).await;
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

    pub async fn cancel(&self, job_id: Uuid) {
        self.send_resolution(wire::VERB_CANCEL, json!({ "job_id": job_id.to_string() }))
            .await;
    }

    pub async fn finish(&self, job_id: Uuid) {
        self.finish_as(Uuid::now_v7(), job_id).await;
    }

    pub async fn finish_as(&self, command_id: Uuid, job_id: Uuid) {
        self.send_resolution_as(
            command_id,
            wire::VERB_FINISH,
            json!({ "job_id": job_id.to_string() }),
        )
        .await;
    }

    pub async fn fail(&self, job_id: Uuid, note: Option<&str>) {
        let mut payload = Map::new();
        payload.insert("job_id".to_owned(), json!(job_id.to_string()));
        if let Some(note) = note {
            payload.insert("note".to_owned(), json!(note));
        }
        self.send_resolution(wire::VERB_FAIL, Value::Object(payload))
            .await;
    }

    async fn send_resolution(&self, verb: &str, payload: Value) {
        self.send_resolution_as(Uuid::now_v7(), verb, payload).await;
    }

    async fn send_resolution_as(&self, command_id: Uuid, verb: &str, payload: Value) {
        let command =
            wire::command_envelope(command_id, verb, Uuid::now_v7(), self.account_id, payload);
        self.send(verb, &command).await;
    }
}
