use bc_jobs::domain::ids::{ItemId, RunId};
use bc_jobs::domain::mail::inbox::ItemKind;
use bc_jobs::ports::PortError;
use br_core_events::EventMetadata;
use br_core_integration::IntegrationEvent;
use br_util_nats_fabric::{OutboxRecord, stage};
use chrono::{DateTime, Utc};
use contract_jobs::event_v2::{EVENT_TYPE_ITEM_ARRIVED, ItemKind as WireKind, RunItemArrived};
use contract_jobs::evt_run_item_arrived_v1_coords;
use sqlx::PgConnection;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Arrival {
    pub run_id: RunId,
    pub address: Option<Uuid>,
    pub item_id: ItemId,
    pub kind: ItemKind,
}

pub struct Ring<'a> {
    pub record_id: Uuid,
    pub event_id: Uuid,
    pub metadata: &'a EventMetadata,
    pub at: DateTime<Utc>,
}

pub async fn ring(
    tx: &mut PgConnection,
    arrival: Arrival,
    ring: Ring<'_>,
) -> Result<(), PortError> {
    let payload = RunItemArrived {
        run_id: arrival.run_id.as_uuid(),
        address: arrival.address,
        item_id: arrival.item_id.as_uuid(),
        item_kind: match arrival.kind {
            ItemKind::Mail => WireKind::Mail,
            ItemKind::CcNotice => WireKind::CcNotice,
            ItemKind::Notice => WireKind::Notice,
        },
    };
    let coords = evt_run_item_arrived_v1_coords().map_err(|error| PortError::Unavailable {
        detail: error.to_string(),
    })?;
    let envelope = IntegrationEvent::new(
        ring.event_id,
        EVENT_TYPE_ITEM_ARRIVED,
        1,
        ring.at,
        ring.metadata.clone(),
        payload,
    );
    let record = OutboxRecord::stage_event(ring.record_id, coords, &envelope).map_err(|error| {
        PortError::Unavailable {
            detail: error.to_string(),
        }
    })?;
    stage(&mut *tx, &record)
        .await
        .map_err(|error| PortError::Unavailable {
            detail: error.to_string(),
        })?;
    Ok(())
}
