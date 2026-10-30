use bc_jobs::ports::environment::IdFactory;
use br_core_events::EventMetadata;
use br_util_nats_fabric::stage;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::app::integration;
use crate::db::PgStore;
use crate::error::ServiceError;

pub struct CreationRefusal<'a> {
    pub job_id: Uuid,
    pub lineage: integration::Lineage,
    pub reason_code: &'a str,
    pub params: serde_json::Value,
}

pub async fn publish_rejection(
    store: &PgStore,
    ids: &dyn IdFactory,
    refusal: CreationRefusal<'_>,
    metadata: &EventMetadata,
    at: DateTime<Utc>,
) -> Result<(), ServiceError> {
    let published = integration::rejection(
        refusal.job_id,
        refusal.lineage,
        refusal.reason_code,
        refusal.params,
    )?;
    stage_all(store, ids, published, metadata, at).await
}

pub async fn publish_command_rejection(
    store: &PgStore,
    ids: &dyn IdFactory,
    rejection: integration::Published,
    metadata: &EventMetadata,
    at: DateTime<Utc>,
) -> Result<(), ServiceError> {
    stage_all(store, ids, vec![rejection], metadata, at).await
}

async fn stage_all(
    store: &PgStore,
    ids: &dyn IdFactory,
    published: Vec<integration::Published>,
    metadata: &EventMetadata,
    at: DateTime<Utc>,
) -> Result<(), ServiceError> {
    let mut tx = store.begin().await?;
    for published in published {
        let record = integration::record(published, ids.next(), ids.next(), metadata, at)?;
        stage(&mut *tx, &record)
            .await
            .map_err(|error| ServiceError::Infra(error.to_string()))?;
    }
    tx.commit().await?;
    Ok(())
}
