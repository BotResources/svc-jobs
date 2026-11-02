use bc_jobs::domain::runner::refusal_notice::RefusalNotice as Told;
use bc_jobs::ports::environment::IdFactory;
use br_core_events::EventMetadata;
use br_util_nats_fabric::stage;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::app::integration;
use crate::db::PgStore;
use crate::db::call_notices::{Delivering, insert_notice};
use crate::db::directory::{address_holder_in, tree_of_run};
use crate::db::notice_target::{DedupKey, NoticeFor};
use crate::db::restart::wake_run;
use crate::db::tree_lock::lock_tree;
use crate::error::ServiceError;

pub struct RefusalNotice {
    pub told: Told,
    pub command: Option<Uuid>,
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
    let refused = match refusal.notice {
        Some(refused) => told_under_lock(&mut tx, refused).await?,
        None => None,
    };
    stage_in(&mut tx, ids, published, metadata, at).await?;
    let mut woke = false;
    if let Some(refused) = refused {
        let caller_run = refused.told.caller_run;
        let written = insert_notice(
            &mut tx,
            NoticeFor::refusal(
                caller_run,
                refused.told.address,
                refused.command.map(DedupKey::of_command),
            ),
            &refused.told.notice,
            Delivering { ids, metadata, at },
        )
        .await?;
        if written.is_some() {
            wake_run(&mut tx, caller_run.as_uuid(), at).await?;
            woke = true;
        }
    }
    tx.commit().await?;
    Ok(woke)
}

async fn told_under_lock(
    tx: &mut sqlx::PgConnection,
    refused: RefusalNotice,
) -> Result<Option<RefusalNotice>, ServiceError> {
    let Some(tree) = tree_of_run(tx, refused.told.caller_run).await? else {
        return Ok(None);
    };
    lock_tree(tx, tree).await?;
    let holder = address_holder_in(tx, refused.told.address.as_uuid()).await?;
    Ok(refused
        .told
        .guard_address_held(holder)
        .is_ok()
        .then_some(refused))
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
