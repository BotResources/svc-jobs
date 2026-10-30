use async_nats::jetstream::stream::RawMessageErrorKind;
use bc_jobs::domain::ids::RunId;
use bc_jobs::domain::keys::{InstanceKey, RunnerTypeKey};
use bc_jobs::ports::PortError;
use contract_jobs::runner as wire;
use contract_jobs::runner_transport::{RunnerStatusFact, status_subject};
use contract_jobs::segment::SubjectSegment;

use super::consume::STATUS_DURABLE;
use super::{RunnerChannels, transport_error};

pub async fn acknowledged_start(
    channels: &RunnerChannels,
    run_id: RunId,
    runner_type: &RunnerTypeKey,
) -> Result<Option<InstanceKey>, PortError> {
    let segment = SubjectSegment::runner_type(runner_type.as_str()).map_err(transport_error)?;
    let subject = status_subject(&segment, RunnerStatusFact::Started);
    let stream = channels
        .context()
        .get_stream(wire::STATUS_STREAM)
        .await
        .map_err(transport_error)?;
    let mut sequence = stream
        .consumer_info(STATUS_DURABLE)
        .await
        .map(|info| info.ack_floor.stream_sequence + 1)
        .unwrap_or(1);
    loop {
        let message = match stream
            .raw_message_builder()
            .next_by_subject(subject.clone())
            .sequence(sequence)
            .send()
            .await
        {
            Ok(message) => message,
            Err(error) if error.kind() == RawMessageErrorKind::NoMessageFound => return Ok(None),
            Err(error) => return Err(transport_error(error)),
        };
        sequence = message.sequence + 1;
        let Ok(fact) = serde_json::from_slice::<wire::RunStarted>(&message.payload) else {
            continue;
        };
        if fact.run_id == run_id.as_uuid() {
            return InstanceKey::new(&fact.instance_key)
                .map(Some)
                .map_err(|error| transport_error(error.code()));
        }
    }
}
