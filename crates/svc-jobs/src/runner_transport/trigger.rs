use async_nats::jetstream::stream::RawMessageErrorKind;
use async_trait::async_trait;
use bc_jobs::domain::ids::RunId;
use bc_jobs::domain::keys::{ReasonCode, RunnerTypeKey};
use bc_jobs::ports::PortError;
use bc_jobs::ports::transport::{RunTrigger, RunnerTransport};
use contract_jobs::runner as wire;
use contract_jobs::runner_transport::trigger_subject;
use contract_jobs::segment::SubjectSegment;
use serde_json::Value;

use super::{RunnerChannels, transport_error};

const WITHDRAWAL_SCAN_LIMIT: u64 = 4_096;

fn segment(runner_type: &RunnerTypeKey) -> Result<SubjectSegment, PortError> {
    SubjectSegment::runner_type(runner_type.as_str()).map_err(transport_error)
}

#[async_trait]
impl RunnerTransport for RunnerChannels {
    async fn dispatch(&self, trigger: &RunTrigger) -> Result<(), PortError> {
        let subject = trigger_subject(&segment(&trigger.runner_type)?);
        let payload = wire::Trigger {
            version: wire::WIRE_VERSION,
            run_id: trigger.run_id.as_uuid(),
            job_id: trigger.job_id.as_uuid(),
            config: trigger
                .config
                .as_ref()
                .map(|config| config.as_value().clone()),
            attempt: trigger.attempt.get(),
            triggered_by: trigger.triggered_by.as_ref().map(|user| user.id().0),
        };
        let bytes = serde_json::to_vec(&payload).map_err(transport_error)?;
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

    async fn withdraw_trigger(
        &self,
        run_id: RunId,
        runner_type: &RunnerTypeKey,
    ) -> Result<(), PortError> {
        let subject = trigger_subject(&segment(runner_type)?);
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
