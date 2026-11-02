use bc_jobs::ports::environment::IdFactory;
use br_core_events::EventMetadata;
use chrono::{DateTime, Utc};

use super::rejection::stage_in;
use crate::app::integration::Published;
use crate::db::PgStore;
use crate::error::ServiceError;

pub async fn publish_again(
    store: &PgStore,
    ids: &dyn IdFactory,
    published: Vec<Published>,
    metadata: &EventMetadata,
    at: DateTime<Utc>,
) -> Result<(), ServiceError> {
    let mut tx = store.begin().await?;
    stage_in(&mut tx, ids, published, metadata, at).await?;
    tx.commit().await?;
    Ok(())
}
