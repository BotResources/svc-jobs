//! A refused job creation, published on its own: nothing is written to the
//! ledger, only the rejection fact leaves through the outbox.

use bc_jobs::ports::environment::IdFactory;
use br_core_events::EventMetadata;
use br_util_nats_fabric::stage;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::app::integration;
use crate::db::PgStore;
use crate::error::ServiceError;

pub async fn publish_rejection(
    store: &PgStore,
    ids: &dyn IdFactory,
    job_id: Uuid,
    reason_code: &str,
    params: serde_json::Value,
    metadata: &EventMetadata,
    at: DateTime<Utc>,
) -> Result<(), ServiceError> {
    let published = integration::rejection(job_id, reason_code, params)?;
    let record = integration::record(published, ids.next(), ids.next(), metadata, at)?;
    let mut tx = store.begin().await?;
    stage(&mut *tx, &record)
        .await
        .map_err(|error| ServiceError::Infra(error.to_string()))?;
    tx.commit().await?;
    Ok(())
}
