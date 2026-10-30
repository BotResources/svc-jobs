mod message;
mod v1;
mod v2;

use bc_jobs::domain::job::Job;
use bc_jobs::event::job::JobEvent;
use br_core_events::EventMetadata;
use br_core_integration::{EventCoords, IntegrationEvent};
use br_util_nats_fabric::OutboxRecord;
use chrono::{DateTime, Utc};
use serde_json::Value;
use uuid::Uuid;

use crate::error::ServiceError;

pub use message::plain_message;
pub use v2::{Lineage, cancel_accepted, command_rejected, creation_rejected};

#[derive(Debug, Clone)]
pub struct Published {
    pub coords: EventCoords,
    pub event_type: &'static str,
    pub schema_version: u8,
    pub payload: Value,
}

pub fn rejection(
    job_id: Uuid,
    lineage: Lineage,
    reason_code: &str,
    params: Value,
) -> Result<Vec<Published>, ServiceError> {
    Ok(vec![
        v1::rejection(job_id, reason_code, params.clone())?,
        creation_rejected(job_id, lineage, reason_code, params)?,
    ])
}

pub fn of_event(
    event: &JobEvent,
    before: Option<&Job>,
    note: Option<&str>,
) -> Result<Vec<Published>, ServiceError> {
    let mut published = Vec::new();
    published.extend(v1::of_event(event, before, note)?);
    published.extend(v2::of_event(event, before, note)?);
    Ok(published)
}

pub fn record(
    published: Published,
    record_id: Uuid,
    event_id: Uuid,
    metadata: &EventMetadata,
    occurred_at: DateTime<Utc>,
) -> Result<OutboxRecord, ServiceError> {
    let envelope = IntegrationEvent::new(
        event_id,
        published.event_type,
        published.schema_version,
        occurred_at,
        metadata.clone(),
        published.payload,
    );
    OutboxRecord::stage_event(record_id, published.coords, &envelope)
        .map_err(|error| ServiceError::Infra(error.to_string()))
}
