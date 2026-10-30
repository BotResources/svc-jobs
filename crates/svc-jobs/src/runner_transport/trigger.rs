use std::collections::HashSet;

use async_nats::jetstream::stream::RawMessageErrorKind;
use async_trait::async_trait;
use bc_jobs::domain::ids::RunId;
use bc_jobs::domain::keys::{InstanceKey, ReasonCode, RunnerTypeKey};
use bc_jobs::domain::payload::{CallMode, JobPayload};
use bc_jobs::ports::PortError;
use bc_jobs::ports::transport::{AnnouncedInstance, RunTrigger, RunnerTransport};
use contract_jobs::command_v3;
use contract_jobs::runner as wire;
use contract_jobs::runner_transport::trigger_v2_subject;
use contract_jobs::segment::SubjectSegment;
use serde_json::Value;

use super::{RunnerChannels, transport_error};

const WITHDRAWAL_SCAN_LIMIT: u64 = 4_096;

fn segment(runner_type: &RunnerTypeKey) -> Result<SubjectSegment, PortError> {
    SubjectSegment::runner_type(runner_type.as_str()).map_err(transport_error)
}

pub fn wire_trigger(trigger: &RunTrigger) -> wire::TriggerV2 {
    wire::TriggerV2 {
        version: wire::WIRE_VERSION_V2,
        run_id: trigger.run_id.as_uuid(),
        job_id: trigger.job_id.as_uuid(),
        attempt: trigger.attempt.get(),
        parent_job_id: trigger.parent_job_id.as_uuid(),
        initial_job_id: trigger.initial_job_id.as_uuid(),
        passport: trigger
            .passport
            .as_ref()
            .map(|passport| passport.document().clone()),
        triggered_by: trigger.triggered_by.as_ref().map(|user| user.id().0),
        config: trigger
            .config
            .as_ref()
            .map(|config| config.as_value().clone()),
        payload: trigger.payload.as_ref().map(wire_payload),
        mode: trigger.call.map(|call| wire_mode(call.mode)),
        caller_address: trigger.call.and_then(|call| call.caller_address),
        restart: trigger.restart_of_run_id.map(|run| wire::RestartBlock {
            restart_of_run_id: run.as_uuid(),
        }),
    }
}

fn wire_mode(mode: CallMode) -> command_v3::CallMode {
    match mode {
        CallMode::Sync => command_v3::CallMode::Sync,
        CallMode::Background => command_v3::CallMode::Background,
    }
}

fn wire_payload(payload: &JobPayload) -> command_v3::Payload {
    match payload {
        JobPayload::Inline {
            message,
            attachments,
        } => command_v3::Payload::Inline {
            message: message.clone(),
            attachments: attachments
                .iter()
                .map(|attachment| command_v3::InlineAttachment {
                    name: attachment.name.clone(),
                    content_type: attachment.content_type.clone(),
                    content: attachment.content.clone(),
                })
                .collect(),
        },
        JobPayload::Kept => command_v3::Payload::Kept,
        JobPayload::Reference {
            service,
            document_id,
        } => command_v3::Payload::Reference {
            service: service.clone(),
            document_id: document_id.clone(),
        },
    }
}

#[async_trait]
impl RunnerTransport for RunnerChannels {
    async fn dispatch(&self, trigger: &RunTrigger) -> Result<(), PortError> {
        let subject = trigger_v2_subject(&segment(&trigger.runner_type)?);
        let bytes = serde_json::to_vec(&wire_trigger(trigger)).map_err(transport_error)?;
        self.context()
            .publish(subject, bytes.into())
            .await
            .map_err(transport_error)?
            .await
            .map_err(transport_error)?;
        Ok(())
    }

    async fn request_stop(&self, run_id: RunId, _reason: &ReasonCode) -> Result<(), PortError> {
        super::cancel::request_stop(self, run_id).await
    }

    async fn withdraw_stop(&self, run_id: RunId) -> Result<(), PortError> {
        super::cancel::withdraw_stop(self, run_id).await
    }

    async fn announced_instances(&self) -> Result<HashSet<AnnouncedInstance>, PortError> {
        super::presence::announced(self).await
    }

    async fn acknowledged_start(
        &self,
        run_id: RunId,
        runner_type: &RunnerTypeKey,
    ) -> Result<Option<InstanceKey>, PortError> {
        super::start_catch_up::acknowledged_start(self, run_id, runner_type).await
    }

    async fn withdraw_trigger(
        &self,
        run_id: RunId,
        runner_type: &RunnerTypeKey,
    ) -> Result<(), PortError> {
        let subject = trigger_v2_subject(&segment(runner_type)?);
        let mut sequence = 1;
        for _ in 0..WITHDRAWAL_SCAN_LIMIT {
            let message = self
                .triggers()
                .raw_message_builder()
                .next_by_subject(subject.clone())
                .sequence(sequence)
                .send()
                .await;
            let message = match message {
                Ok(message) => message,
                Err(error) if error.kind() == RawMessageErrorKind::NoMessageFound => {
                    return Ok(());
                }
                Err(error) => return Err(transport_error(error)),
            };
            let carried: Value = serde_json::from_slice(&message.payload).unwrap_or(Value::Null);
            if carried["run_id"] == serde_json::json!(run_id.as_uuid().to_string()) {
                self.triggers()
                    .delete_message(message.sequence)
                    .await
                    .map_err(transport_error)?;
                return Ok(());
            }
            sequence = message.sequence + 1;
        }
        tracing::warn!(
            run_id = %run_id.as_uuid(),
            runner_type = runner_type.as_str(),
            scanned = WITHDRAWAL_SCAN_LIMIT,
            "the undelivered trigger of a cancelled run was not found within the scan limit; \
             a runner may still claim it and its start will be refused by the terminal job"
        );
        Ok(())
    }
}
