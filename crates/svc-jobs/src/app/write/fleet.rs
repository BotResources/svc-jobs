use bc_jobs::domain::actions::fleet::RetirementWindow;
use bc_jobs::domain::fleet::RunnerType;
use bc_jobs::domain::ids::{EventId, RunnerTypeId};
use bc_jobs::domain::keys::RunnerTypeKey;
use bc_jobs::event::fleet::FleetEvent;
use bc_jobs::ports::environment::IdFactory;
use br_core_events::EventMetadata;
use chrono::{DateTime, Utc};

use crate::app::fingerprint;
use crate::db::{PgStore, apply};
use crate::error::ServiceError;

pub struct FleetChange<'a> {
    pub runner_type_id: RunnerTypeId,
    pub runner_type: &'a RunnerTypeKey,
    pub decided_on: Option<&'a RunnerType>,
    pub events: &'a [FleetEvent],
}

pub async fn commit_fleet_events(
    store: &PgStore,
    ids: &dyn IdFactory,
    change: FleetChange<'_>,
    metadata: &EventMetadata,
    at: DateTime<Utc>,
) -> Result<(), ServiceError> {
    commit_fleet(store, ids, change, None, metadata, at).await
}

pub async fn commit_fleet(
    store: &PgStore,
    ids: &dyn IdFactory,
    change: FleetChange<'_>,
    revalidate_retirement_within: Option<RetirementWindow>,
    metadata: &EventMetadata,
    at: DateTime<Utc>,
) -> Result<(), ServiceError> {
    if change.events.is_empty() {
        return Ok(());
    }
    let mut tx = store.begin().await?;
    let locked =
        PgStore::lock_runner_type(&mut tx, change.runner_type_id, change.runner_type).await?;
    if fingerprint::of_fleet(change.decided_on) != fingerprint::of_fleet(locked.as_ref()) {
        return Err(ServiceError::write_race());
    }
    if let Some(window) = revalidate_retirement_within {
        let locked = locked.as_ref().ok_or_else(ServiceError::write_race)?;
        let facts =
            PgStore::runner_type_decision_facts_in(&mut tx, change.runner_type, window).await?;
        locked.guard_retire(facts)?;
    }
    let mut recorded = Vec::with_capacity(change.events.len());
    for event in change.events {
        recorded.push((EventId::new(ids.next())?, event.clone()));
    }
    apply::apply_fleet_events(&mut tx, change.runner_type_id, &recorded, metadata, at).await?;
    tx.commit().await?;
    Ok(())
}
