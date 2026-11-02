use bc_jobs::domain::ids::{AgentId, RunId};
use bc_jobs::domain::mail::inbox::Notice;
use bc_jobs::ports::environment::IdFactory;
use br_core_events::EventMetadata;
use br_util_nats_fabric::stage;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::app::integration;
use crate::db::PgStore;
use crate::db::call_notices::{Delivering, NoticeFor, insert_notice};
use crate::db::restart::wake_run;
use crate::error::ServiceError;

pub struct RefusalNotice {
    pub caller_run: RunId,
    pub address: AgentId,
    pub source_key: Option<String>,
    pub notice: Notice,
}

pub struct CreationRefusal<'a> {
    pub job_id: Uuid,
    pub lineage: integration::Lineage,
    pub reason_code: &'a str,
    pub params: serde_json::Value,
    pub notice: Option<RefusalNotice>,
}

pub async fn publish_rejection(
    store: &PgStore,
    ids: &dyn IdFactory,
    refusal: CreationRefusal<'_>,
    metadata: &EventMetadata,
    at: DateTime<Utc>,
) -> Result<bool, ServiceError> {
    let published = integration::rejection(
        refusal.job_id,
        refusal.lineage,
        refusal.reason_code,
        refusal.params,
    )?;
    let mut tx = store.begin().await?;
    stage_in(&mut tx, ids, published, metadata, at).await?;
    let mut woke = false;
    if let Some(refused) = refusal.notice {
        let written = insert_notice(
            &mut tx,
            NoticeFor::refusal(refused.caller_run, refused.address, refused.source_key),
            &refused.notice,
            Delivering { ids, metadata, at },
        )
        .await?;
        if written.is_some() {
            wake_run(&mut tx, refused.caller_run.as_uuid(), at).await?;
            woke = true;
        }
    }
    tx.commit().await?;
    Ok(woke)
}

pub async fn publish_command_rejection(
    store: &PgStore,
    ids: &dyn IdFactory,
    rejection: integration::Published,
    metadata: &EventMetadata,
    at: DateTime<Utc>,
) -> Result<(), ServiceError> {
    let mut tx = store.begin().await?;
    stage_in(&mut tx, ids, vec![rejection], metadata, at).await?;
    tx.commit().await?;
    Ok(())
}

async fn stage_in(
    tx: &mut sqlx::PgConnection,
    ids: &dyn IdFactory,
    published: Vec<integration::Published>,
    metadata: &EventMetadata,
    at: DateTime<Utc>,
) -> Result<(), ServiceError> {
    for published in published {
        let record = integration::record(published, ids.next(), ids.next(), metadata, at)?;
        stage(&mut *tx, &record)
            .await
            .map_err(|error| ServiceError::Infra(error.to_string()))?;
    }
    Ok(())
}
