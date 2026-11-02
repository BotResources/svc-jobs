use bc_jobs::domain::ids::{ItemId, RunId};
use bc_jobs::domain::mail::inbox::ItemKind;
use bc_jobs::ports::PortError;
use bc_jobs::ports::environment::IdFactory;
use br_core_events::EventMetadata;
use br_core_integration::{CoordError, EventCoords, IntegrationEvent};
use br_util_nats_fabric::{OutboxRecord, stage};
use chrono::{DateTime, Utc};
use contract_jobs::event_v2::{
    EVENT_TYPE_ITEM_ARRIVED, EVENT_V2_SCHEMA_VERSION, ItemKind as WireKind,
    ItemKindV2 as WireKindV2, RunItemArrived, RunItemArrivedV2,
};
use contract_jobs::{evt_run_item_arrived_v1_coords, evt_run_item_arrived_v2_coords};
use serde::Serialize;
use sqlx::PgConnection;
use uuid::Uuid;

const SCHEMA_VERSION_V1: u8 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Arrival {
    pub run_id: RunId,
    pub address: Option<Uuid>,
    pub item_id: ItemId,
    pub kind: ItemKind,
}

pub struct Ring<'a> {
    pub ids: &'a dyn IdFactory,
    pub metadata: &'a EventMetadata,
    pub at: DateTime<Utc>,
}

fn wire_kind_v2(kind: ItemKind) -> WireKindV2 {
    match kind {
        ItemKind::Mail => WireKindV2::Mail,
        ItemKind::CcNotice => WireKindV2::CcNotice,
        ItemKind::Notice => WireKindV2::Notice,
        ItemKind::MailNotice => WireKindV2::MailNotice,
    }
}

fn wire_kind(kind: ItemKind) -> Option<WireKind> {
    match kind {
        ItemKind::Mail => Some(WireKind::Mail),
        ItemKind::CcNotice => Some(WireKind::CcNotice),
        ItemKind::Notice => Some(WireKind::Notice),
        ItemKind::MailNotice => None,
    }
}

pub async fn ring(
    tx: &mut PgConnection,
    arrival: Arrival,
    ring: Ring<'_>,
) -> Result<(), PortError> {
    if let Some(item_kind) = wire_kind(arrival.kind) {
        let payload = RunItemArrived {
            run_id: arrival.run_id.as_uuid(),
            address: arrival.address,
            item_id: arrival.item_id.as_uuid(),
            item_kind,
        };
        stage_ring(
            tx,
            evt_run_item_arrived_v1_coords(),
            SCHEMA_VERSION_V1,
            payload,
            &ring,
        )
        .await?;
    }
    let payload = RunItemArrivedV2 {
        run_id: arrival.run_id.as_uuid(),
        address: arrival.address,
        item_id: arrival.item_id.as_uuid(),
        item_kind: wire_kind_v2(arrival.kind),
    };
    stage_ring(
        tx,
        evt_run_item_arrived_v2_coords(),
        EVENT_V2_SCHEMA_VERSION,
        payload,
        &ring,
    )
    .await
}

async fn stage_ring<P: Serialize>(
    tx: &mut PgConnection,
    coords: Result<EventCoords, CoordError>,
    schema_version: u8,
    payload: P,
    ring: &Ring<'_>,
) -> Result<(), PortError> {
    let unavailable = |detail: String| PortError::Unavailable { detail };
    let coords = coords.map_err(|error| unavailable(error.to_string()))?;
    let envelope = IntegrationEvent::new(
        ring.ids.next(),
        EVENT_TYPE_ITEM_ARRIVED,
        schema_version,
        ring.at,
        ring.metadata.clone(),
        payload,
    );
    let record = OutboxRecord::stage_event(ring.ids.next(), coords, &envelope)
        .map_err(|error| unavailable(error.to_string()))?;
    stage(&mut *tx, &record)
        .await
        .map_err(|error| unavailable(error.to_string()))
}
