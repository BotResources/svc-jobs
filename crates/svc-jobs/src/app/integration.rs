use bc_jobs::domain::job::Job;
use bc_jobs::event::job::JobEvent;
use br_core_events::EventMetadata;
use br_core_integration::{EventCoords, IntegrationEvent};
use br_util_nats_fabric::OutboxRecord;
use chrono::{DateTime, Utc};
use contract_jobs::event as wire;
use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

use crate::error::ServiceError;

pub struct Published {
    pub coords: EventCoords,
    pub event_type: &'static str,
    pub payload: Value,
}

pub fn rejection(
    job_id: Uuid,
    reason_code: &str,
    params: Value,
) -> Result<Published, ServiceError> {
    Ok(Published {
        coords: contract_jobs::evt_job_creation_rejected_v1_coords()
            .map_err(|error| ServiceError::Infra(error.to_string()))?,
        event_type: wire::EVENT_TYPE_CREATION_REJECTED,
        payload: serde_json::to_value(wire::JobCreationRejected {
            job_id,
            reason_code: reason_code.to_owned(),
            params,
        })?,
    })
}

pub fn of_event(
    event: &JobEvent,
    before: Option<&Job>,
    note: Option<&str>,
) -> Result<Option<Published>, ServiceError> {
    let published = match event {
        JobEvent::JobQueued(fact) => Some(published(
            contract_jobs::evt_job_queued_v1_coords(),
            wire::EVENT_TYPE_QUEUED,
            &wire::JobQueued {
                job_id: fact.job_id.as_uuid(),
                runner_type: fact.runner_type.as_str().to_owned(),
            },
        )?),
        JobEvent::RunStarted(fact) if is_first_attempt(before, fact.run_id) => Some(published(
            contract_jobs::evt_job_started_v1_coords(),
            wire::EVENT_TYPE_STARTED,
            &wire::JobStarted {
                job_id: fact.job_id.as_uuid(),
                run_id: fact.run_id.as_uuid(),
            },
        )?),
        JobEvent::RunPlanDeclared(fact) => Some(published(
            contract_jobs::evt_job_plan_declared_v1_coords(),
            wire::EVENT_TYPE_PLAN_DECLARED,
            &wire::JobPlanDeclared {
                job_id: fact.job_id.as_uuid(),
                run_id: fact.run_id.as_uuid(),
                steps: fact
                    .items
                    .iter()
                    .map(|item| item.label().as_str().to_owned())
                    .collect(),
            },
        )?),
        JobEvent::RunStepStarted(fact) => Some(published(
            contract_jobs::evt_job_step_started_v1_coords(),
            wire::EVENT_TYPE_STEP_STARTED,
            &wire::JobStepStarted {
                job_id: fact.job_id.as_uuid(),
                run_id: fact.run_id.as_uuid(),
                index: fact.step_index.get(),
                label: fact.label.as_str().to_owned(),
                started_at: fact.started_at,
            },
        )?),
        JobEvent::JobCompleted(fact) => Some(published(
            contract_jobs::evt_job_completed_v1_coords(),
            wire::EVENT_TYPE_COMPLETED,
            &wire::JobCompleted {
                job_id: fact.job_id.as_uuid(),
            },
        )?),
        JobEvent::JobFailed(fact) => Some(published(
            contract_jobs::evt_job_failed_v1_coords(),
            wire::EVENT_TYPE_FAILED,
            &wire::JobFailed {
                job_id: fact.job_id.as_uuid(),
                failure_cause: fact.failure_cause.as_db_str().to_owned(),
                failure_report: fact.report.as_ref().map(|report| wire::FailureReport {
                    kind: report.kind().as_db_str().to_owned(),
                    reason_code: report.reason_code().as_str().to_owned(),
                    params: report.params().clone(),
                    diagnostic: report.diagnostic().clone(),
                }),
                note: note.map(str::to_owned),
            },
        )?),
        JobEvent::JobCancelled(fact) => Some(published(
            contract_jobs::evt_job_cancelled_v1_coords(),
            wire::EVENT_TYPE_CANCELLED,
            &wire::JobCancelled {
                job_id: fact.job_id.as_uuid(),
            },
        )?),
        _ => None,
    };
    Ok(published)
}

fn is_first_attempt(before: Option<&Job>, run_id: bc_jobs::domain::ids::RunId) -> bool {
    before
        .and_then(|job| job.find_run(run_id).ok())
        .is_some_and(|run| run.attempt_number().is_first())
}

fn published<T: Serialize>(
    coords: Result<EventCoords, br_core_integration::CoordError>,
    event_type: &'static str,
    payload: &T,
) -> Result<Published, ServiceError> {
    Ok(Published {
        coords: coords.map_err(|error| ServiceError::Infra(error.to_string()))?,
        event_type,
        payload: serde_json::to_value(payload)?,
    })
}

pub fn record(
    published: Published,
    record_id: Uuid,
    event_id: Uuid,
    metadata: &EventMetadata,
    occurred_at: DateTime<Utc>,
) -> Result<OutboxRecord, ServiceError> {
    let envelope = IntegrationEvent::new(
        event_id,
        published.event_type,
        wire::SCHEMA_VERSION,
        occurred_at,
        metadata.clone(),
        published.payload,
    );
    OutboxRecord::stage_event(record_id, published.coords, &envelope)
        .map_err(|error| ServiceError::Infra(error.to_string()))
}
