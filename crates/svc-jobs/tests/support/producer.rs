use br_core_integration::IntegrationCommand;
use br_test_harness::FabricTestNats;
use serde_json::Value;
use uuid::Uuid;

use super::wire;

mod declaration;
mod resolution;

pub use declaration::JobDeclaration;

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
        wire::command_envelope_at(
            Uuid::now_v7(),
            wire::VERB_CREATE,
            wire::CONTRACT_V3,
            Uuid::now_v7(),
            self.account_id,
            declaration.payload(&self.bc),
        )
    }

    pub async fn send(&self, verb: &str, command: &IntegrationCommand<Value>) {
        let version = if verb == wire::VERB_CREATE {
            wire::CONTRACT_V3
        } else {
            wire::CONTRACT_V1
        };
        self.send_at(verb, version, command).await;
    }

    pub async fn send_at(&self, verb: &str, version: u8, command: &IntegrationCommand<Value>) {
        self.fabric
            .fabric()
            .publish_command(&wire::command_coords_at(verb, version), command)
            .await
            .unwrap_or_else(|error| {
                panic!("publishing jobs.job.{verb}.v{version} onto the fabric: {error}")
            });
    }

    pub async fn declare(&self, declaration: &JobDeclaration) -> IntegrationCommand<Value> {
        let command = self.create_command(declaration);
        self.send(wire::VERB_CREATE, &command).await;
        command
    }

    pub async fn declare_v2(&self, declaration: &JobDeclaration) -> IntegrationCommand<Value> {
        self.declare(declaration).await
    }

    pub async fn send_v3(&self, verb: &str, payload: Value) -> IntegrationCommand<Value> {
        let command = wire::command_envelope_at(
            Uuid::now_v7(),
            verb,
            wire::CONTRACT_V3,
            Uuid::now_v7(),
            self.account_id,
            payload,
        );
        self.send_at(verb, wire::CONTRACT_V3, &command).await;
        command
    }

    pub async fn send_v3_as(
        &self,
        command_id: Uuid,
        correlation_id: Uuid,
        verb: &str,
        payload: Value,
    ) -> IntegrationCommand<Value> {
        let command = wire::command_envelope_at(
            command_id,
            verb,
            wire::CONTRACT_V3,
            correlation_id,
            self.account_id,
            payload,
        );
        self.send_at(verb, wire::CONTRACT_V3, &command).await;
        command
    }

    pub async fn send_v1(&self, verb: &str, payload: Value) -> IntegrationCommand<Value> {
        let command = wire::command_envelope_at(
            Uuid::now_v7(),
            verb,
            wire::CONTRACT_V1,
            Uuid::now_v7(),
            self.account_id,
            payload,
        );
        self.send_at(verb, wire::CONTRACT_V1, &command).await;
        command
    }

    pub async fn declare_payload(&self, payload: Value) {
        let command = wire::command_envelope_at(
            Uuid::now_v7(),
            wire::VERB_CREATE,
            wire::CONTRACT_V3,
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
}
