use bc_jobs::domain::ids::{EventId, JobId, RunnerTypeId};
use bc_jobs::domain::job::Job;
use bc_jobs::event::fleet::FleetEvent;
use bc_jobs::event::job::JobEvent;
use br_core_events::EventMetadata;
use br_util_nats_fabric::stage;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::app::integration;
use crate::db::{PgStore, apply};
use crate::error::ServiceError;

pub struct JobChange {
    pub job_id: JobId,
    pub before: Option<Job>,
    pub events: Vec<JobEvent>,
    pub note: Option<String>,
}

impl JobChange {
    pub fn new(job_id: JobId, before: Option<Job>, events: Vec<JobEvent>) -> Self {
        Self {
            job_id,
            before,
            events,
            note: None,
        }
    }

    pub fn with_note(mut self, note: Option<String>) -> Self {
        self.note = note;
        self
    }
}

pub async fn commit_job_changes(
    store: &PgStore,
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
    for change in &changes {
        if change.before.is_some() {
            PgStore::lock_job(&mut tx, change.job_id).await?;
        }
        let recorded: Vec<(EventId, JobEvent)> = change
            .events
            .iter()
            .map(|event| Ok((EventId::new(Uuid::now_v7())?, event.clone())))
            .collect::<Result<_, bc_jobs::JobsError>>()?;
        apply::apply_job_events(&mut tx, change.job_id, &recorded, metadata, at).await?;
        for (event_id, event) in &recorded {
            if let Some(published) =
                integration::of_event(event, change.before.as_ref(), change.note.as_deref(), at)?
            {
                let record = integration::record(published, event_id.as_uuid(), metadata, at)?;
                stage(&mut *tx, &record)
                    .await
                    .map_err(|error| ServiceError::Infra(error.to_string()))?;
            }
        }
    }
    tx.commit().await?;
    Ok(())
}

pub async fn publish_rejection(
    store: &PgStore,
    job_id: Uuid,
    reason_code: &str,
    params: serde_json::Value,
    metadata: &EventMetadata,
    at: DateTime<Utc>,
) -> Result<(), ServiceError> {
    let published = integration::rejection(job_id, reason_code, params)?;
    let record = integration::record(published, Uuid::now_v7(), metadata, at)?;
    let mut tx = store.begin().await?;
    stage(&mut *tx, &record)
        .await
        .map_err(|error| ServiceError::Infra(error.to_string()))?;
    tx.commit().await?;
    Ok(())
}

pub async fn commit_fleet_events(
    store: &PgStore,
    runner_type_id: RunnerTypeId,
    events: &[FleetEvent],
    metadata: &EventMetadata,
    at: DateTime<Utc>,
) -> Result<(), ServiceError> {
    if events.is_empty() {
        return Ok(());
    }
    let mut tx = store.begin().await?;
    let mut recorded = Vec::with_capacity(events.len());
    for event in events {
        recorded.push((EventId::new(Uuid::now_v7())?, event.clone()));
    }
    apply::apply_fleet_events(&mut tx, runner_type_id, &recorded, metadata, at).await?;
    tx.commit().await?;
    Ok(())
}
