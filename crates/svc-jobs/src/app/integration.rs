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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_terminal_parent_is_refused_under_parent_terminal_in_v2_and_keeps_its_v1_code() {
        let job_id = Uuid::now_v7();
        let lineage = Lineage::declared(job_id, None, None);
        let published = rejection(job_id, lineage, "parent_job_terminal", json!({})).unwrap();
        let codes: Vec<(u8, String)> = published
            .iter()
            .map(|event| {
                (
                    event.schema_version,
                    event.payload["reason_code"].as_str().unwrap().to_owned(),
                )
            })
            .collect();
        assert!(codes.contains(&(1, "parent_job_terminal".to_owned())));
        assert!(codes.contains(&(2, "parent_terminal".to_owned())));
        let v2 = published
            .iter()
            .find(|event| event.schema_version == 2)
            .unwrap();
        assert_eq!(v2.payload["message"], "The parent job has already ended.");
    }
}
