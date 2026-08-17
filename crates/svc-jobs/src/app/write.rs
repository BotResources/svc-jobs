use std::collections::{BTreeSet, HashMap};

use bc_jobs::domain::fleet::RunnerType;
use bc_jobs::domain::ids::{EventId, JobId, RunnerTypeId};
use bc_jobs::domain::job::Job;
use bc_jobs::domain::keys::RunnerTypeKey;
use bc_jobs::domain::references::SourceReference;
use bc_jobs::event::fleet::FleetEvent;
use bc_jobs::event::job::JobEvent;
use bc_jobs::ports::environment::IdFactory;
use br_core_events::EventMetadata;
use br_util_nats_fabric::stage;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::app::{fingerprint, followups, integration};
use crate::db::{PgStore, apply, hydrate};
use crate::error::ServiceError;

pub struct JobChange {
    pub job_id: JobId,
    pub before: Option<Job>,
    pub events: Vec<JobEvent>,
    pub note: Option<String>,
    pub claimed_source: Option<SourceReference>,
}

impl JobChange {
    pub fn new(job_id: JobId, before: Option<Job>, events: Vec<JobEvent>) -> Self {
        Self {
            job_id,
            before,
            events,
            note: None,
            claimed_source: None,
        }
    }

    pub fn with_note(mut self, note: Option<String>) -> Self {
        self.note = note;
        self
    }

    pub fn claiming_source(mut self, source: Option<SourceReference>) -> Self {
        self.claimed_source = source;
        self
    }
}

pub async fn commit_job_changes(
    store: &PgStore,
    ids: &dyn IdFactory,
    changes: Vec<JobChange>,
    metadata: &EventMetadata,
    at: DateTime<Utc>,
) -> Result<(), ServiceError> {
    let changes: Vec<JobChange> = changes
        .into_iter()
        .filter(|change| !change.events.is_empty())
        .collect();
    if changes.is_empty() {
        return Ok(());
    }
    let mut tx = store.begin().await?;
    let held = followups::rows_to_lock(&mut tx, &changes).await?;
    let locked = lock_in_ascending_order(&mut tx, held.ids()).await?;
    for change in changes.iter().filter(|change| change.before.is_some()) {
        guard_decision_still_holds(change.before.as_ref(), locked.get(&change.job_id.as_uuid()))?;
    }
    for change in &changes {
        apply_change(&mut tx, ids, change, metadata, at).await?;
    }
    for followup in followups::of_settled_jobs(&mut tx, &changes, &held).await? {
        apply_change(&mut tx, ids, &followup, metadata, at).await?;
    }
    tx.commit().await?;
    Ok(())
}

async fn lock_in_ascending_order(
    tx: &mut sqlx::PgConnection,
    rows: &BTreeSet<Uuid>,
) -> Result<HashMap<Uuid, Job>, ServiceError> {
    let mut held = Vec::with_capacity(rows.len());
    for row in rows {
        if let Some(locked) = PgStore::lock_job_row(&mut *tx, JobId::new(*row)?).await? {
            held.push(locked);
        }
    }
    Ok(hydrate::load_map(tx, &held).await?)
}

async fn apply_change(
    tx: &mut sqlx::PgConnection,
    ids: &dyn IdFactory,
    change: &JobChange,
    metadata: &EventMetadata,
    at: DateTime<Utc>,
) -> Result<(), ServiceError> {
    if let Some(source) = &change.claimed_source {
        claim_source(tx, change.job_id, source).await?;
    }
    let recorded: Vec<(EventId, JobEvent)> = change
        .events
        .iter()
        .map(|event| Ok((EventId::new(ids.next())?, event.clone())))
        .collect::<Result<_, bc_jobs::JobsError>>()?;
    apply::apply_job_events(tx, change.job_id, &recorded, metadata, at).await?;
    for (event_id, event) in &recorded {
        if let Some(published) =
            integration::of_event(event, change.before.as_ref(), change.note.as_deref())?
        {
            let record =
                integration::record(published, ids.next(), event_id.as_uuid(), metadata, at)?;
            stage(&mut *tx, &record)
                .await
                .map_err(|error| ServiceError::Infra(error.to_string()))?;
        }
    }
    Ok(())
}

fn guard_decision_still_holds(
    decided_on: Option<&Job>,
    locked: Option<&Job>,
) -> Result<(), ServiceError> {
    let (Some(decided_on), Some(locked)) = (decided_on, locked) else {
        return Err(ServiceError::Contended);
    };
    if fingerprint::of_job(decided_on) == fingerprint::of_job(locked) {
        Ok(())
    } else {
        Err(ServiceError::Contended)
    }
}

async fn claim_source(
    tx: &mut sqlx::PgConnection,
    job_id: JobId,
    source: &SourceReference,
) -> Result<(), ServiceError> {
    apply::refs::lock_source_entity(tx, source.bc(), source.entity_id().as_uuid()).await?;
    match PgStore::active_job_for_source_in(tx, source).await? {
        Some(active) if active != job_id.as_uuid() => Err(ServiceError::Domain(
            bc_jobs::JobsError::SourceAlreadyActive {
                active_job_id: active,
            },
        )),
        _ => Ok(()),
    }
}

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
    if change.events.is_empty() {
        return Ok(());
    }
    let mut tx = store.begin().await?;
    let locked =
        PgStore::lock_runner_type(&mut tx, change.runner_type_id, change.runner_type).await?;
    if fingerprint::of_fleet(change.decided_on) != fingerprint::of_fleet(locked.as_ref()) {
        return Err(ServiceError::Contended);
    }
    let mut recorded = Vec::with_capacity(change.events.len());
    for event in change.events {
        recorded.push((EventId::new(ids.next())?, event.clone()));
    }
    apply::apply_fleet_events(&mut tx, change.runner_type_id, &recorded, metadata, at).await?;
    tx.commit().await?;
    Ok(())
}
