use bc_jobs::domain::ids::{EventId, JobId, RunnerTypeId};
use bc_jobs::domain::job::Job;
use bc_jobs::domain::references::SourceReference;
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
    let mut hydrated: Vec<&JobChange> = changes
        .iter()
        .filter(|change| change.before.is_some())
        .collect();
    hydrated.sort_by_key(|change| change.job_id.as_uuid());
    for change in hydrated {
        let locked = PgStore::lock_job(&mut tx, change.job_id).await?;
        guard_decision_still_holds(change.before.as_ref(), locked.as_ref())?;
    }
    for change in &changes {
        if let Some(source) = &change.claimed_source {
            claim_source(&mut tx, change.job_id, source).await?;
        }
        let recorded: Vec<(EventId, JobEvent)> = change
            .events
            .iter()
            .map(|event| Ok((EventId::new(Uuid::now_v7())?, event.clone())))
            .collect::<Result<_, bc_jobs::JobsError>>()?;
        apply::apply_job_events(&mut tx, change.job_id, &recorded, metadata, at).await?;
        for (event_id, event) in &recorded {
            if let Some(published) =
                integration::of_event(event, change.before.as_ref(), change.note.as_deref())?
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

fn guard_decision_still_holds(
    decided_on: Option<&Job>,
    locked: Option<&Job>,
) -> Result<(), ServiceError> {
    let (Some(decided_on), Some(locked)) = (decided_on, locked) else {
        return Err(ServiceError::Contended);
    };
    if decision_fingerprint(decided_on) == decision_fingerprint(locked) {
        Ok(())
    } else {
        Err(ServiceError::Contended)
    }
}

fn decision_fingerprint(job: &Job) -> (Option<Uuid>, bool, usize) {
    (
        job.resolution().map(|resolution| resolution.id().as_uuid()),
        job.is_deleted(),
        job.runs().len(),
    )
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
