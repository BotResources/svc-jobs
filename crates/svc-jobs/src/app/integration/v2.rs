use bc_jobs::domain::ids::JobId;
use bc_jobs::domain::job::Job;
use bc_jobs::domain::requester::Requester;
use bc_jobs::event::job::JobEvent;
use contract_jobs::event::FailureReport;
use contract_jobs::event_v2 as wire;
use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

use super::Published;
use super::message::{failure_message, plain_message};
use crate::error::ServiceError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Lineage {
    pub parent_job_id: Uuid,
    pub initial_job_id: Uuid,
}

impl Lineage {
    pub fn of_job(job: &Job) -> Self {
        Self {
            parent_job_id: job.wire_parent_job_id().as_uuid(),
            initial_job_id: job.initial_job_id().as_uuid(),
        }
    }

    pub fn declared(job_id: Uuid, parent: Option<Uuid>, initial: Option<Uuid>) -> Self {
        let parent_job_id = parent.unwrap_or(job_id);
        Self {
            parent_job_id,
            initial_job_id: initial.unwrap_or(parent_job_id),
        }
    }
}

pub fn requester_ref(requester: &Requester, service_key: &str) -> wire::RequesterRef {
    match requester {
        Requester::Service(_) => wire::RequesterRef::Service {
            service_key: service_key.to_owned(),
        },
        Requester::Run(run_id) => wire::RequesterRef::Run {
            run_id: run_id.as_uuid(),
        },
    }
}

fn v2_reason_code(domain_code: &str) -> &str {
    match domain_code {
        "parent_job_terminal" => wire::REASON_PARENT_TERMINAL,
        other => other,
    }
}

pub fn creation_rejected(
    job_id: Uuid,
    lineage: Lineage,
    reason_code: &str,
    params: Value,
) -> Result<Published, ServiceError> {
    let reason_code = v2_reason_code(reason_code);
    published(
        contract_jobs::evt_job_creation_rejected_v2_coords(),
        contract_jobs::event::EVENT_TYPE_CREATION_REJECTED,
        wire::EVENT_V2_SCHEMA_VERSION,
        &wire::JobCreationRejectedV2 {
            job_id,
            parent_job_id: lineage.parent_job_id,
            initial_job_id: lineage.initial_job_id,
            reason_code: reason_code.to_owned(),
            message: plain_message(reason_code),
            params: Some(params),
        },
    )
}

pub fn command_rejected(
    command_type: &str,
    job_id: Uuid,
    lineage: Option<Lineage>,
    reason_code: &str,
    params: Value,
) -> Result<Published, ServiceError> {
    published(
        contract_jobs::evt_job_command_rejected_v1_coords(),
        wire::EVENT_TYPE_COMMAND_REJECTED,
        wire::EVENT_V1_NEW_SCHEMA_VERSION,
        &wire::JobCommandRejected {
            command_type: command_type.to_owned(),
            job_id,
            parent_job_id: lineage.map(|lineage| lineage.parent_job_id),
            initial_job_id: lineage.map(|lineage| lineage.initial_job_id),
            reason_code: reason_code.to_owned(),
            message: plain_message(reason_code),
            params: Some(params),
        },
    )
}

pub fn cancel_accepted(job_id: Uuid, lineage: Lineage) -> Result<Published, ServiceError> {
    published(
        contract_jobs::evt_job_cancel_accepted_v1_coords(),
        wire::EVENT_TYPE_CANCEL_ACCEPTED,
        wire::EVENT_V1_NEW_SCHEMA_VERSION,
        &wire::JobCancelAccepted {
            job_id,
            parent_job_id: lineage.parent_job_id,
            initial_job_id: lineage.initial_job_id,
        },
    )
}

pub fn of_event(
    event: &JobEvent,
    before: Option<&Job>,
    note: Option<&str>,
) -> Result<Vec<Published>, ServiceError> {
    let published = match event {
        JobEvent::JobQueued(fact) => {
            let job_id = fact.job_id.as_uuid();
            let lineage = Lineage {
                parent_job_id: fact.parent_job_id.map_or(job_id, |id: JobId| id.as_uuid()),
                initial_job_id: fact.initial_job_id.map_or(job_id, |id: JobId| id.as_uuid()),
            };
            let requester = fact.requester.unwrap_or(Requester::Service(None));
            Some(published(
                contract_jobs::evt_job_queued_v2_coords(),
                contract_jobs::event::EVENT_TYPE_QUEUED,
                wire::EVENT_V2_SCHEMA_VERSION,
                &wire::JobQueuedV2 {
                    job_id,
                    parent_job_id: lineage.parent_job_id,
                    initial_job_id: lineage.initial_job_id,
                    runner_type: fact.runner_type.as_str().to_owned(),
                    requester: requester_ref(&requester, fact.producer.as_str()),
                    restart_of_job_id: fact.restart_of_job_id.map(|id: JobId| id.as_uuid()),
                    retry_of_job_id: fact.predecessor_job_id.map(|id: JobId| id.as_uuid()),
                },
            )?)
        }
        other => of_known_job_event(other, before, note)?,
    };
    Ok(published.into_iter().collect())
}

fn of_known_job_event(
    event: &JobEvent,
    before: Option<&Job>,
    note: Option<&str>,
) -> Result<Option<Published>, ServiceError> {
    let Some(job) = before else {
        return Ok(None);
    };
    let lineage = Lineage::of_job(job);
    let job_id = job.id().as_uuid();
    Ok(match event {
        JobEvent::RunStarted(fact) if first_attempt(job, fact.run_id) => Some(published(
            contract_jobs::evt_job_started_v2_coords(),
            contract_jobs::event::EVENT_TYPE_STARTED,
            wire::EVENT_V2_SCHEMA_VERSION,
            &wire::JobStartedV2 {
                job_id,
                parent_job_id: lineage.parent_job_id,
                initial_job_id: lineage.initial_job_id,
                run_id: fact.run_id.as_uuid(),
            },
        )?),
        JobEvent::RunPlanDeclared(fact) => Some(published(
            contract_jobs::evt_job_plan_declared_v2_coords(),
            contract_jobs::event::EVENT_TYPE_PLAN_DECLARED,
            wire::EVENT_V2_SCHEMA_VERSION,
            &wire::JobPlanDeclaredV2 {
                job_id,
                parent_job_id: lineage.parent_job_id,
                initial_job_id: lineage.initial_job_id,
                run_id: fact.run_id.as_uuid(),
                steps: fact
                    .items
                    .iter()
                    .map(|item| item.label().as_str().to_owned())
                    .collect(),
            },
        )?),
        JobEvent::RunStepStarted(fact) => Some(published(
            contract_jobs::evt_job_step_started_v2_coords(),
            contract_jobs::event::EVENT_TYPE_STEP_STARTED,
            wire::EVENT_V2_SCHEMA_VERSION,
            &wire::JobStepStartedV2 {
                job_id,
                parent_job_id: lineage.parent_job_id,
                initial_job_id: lineage.initial_job_id,
                run_id: fact.run_id.as_uuid(),
                index: fact.step_index.get(),
                label: fact.label.as_str().to_owned(),
                started_at: fact.started_at,
            },
        )?),
        JobEvent::JobCompleted(_) => Some(published(
            contract_jobs::evt_job_completed_v2_coords(),
            contract_jobs::event::EVENT_TYPE_COMPLETED,
            wire::EVENT_V2_SCHEMA_VERSION,
            &wire::JobCompletedV2 {
                job_id,
                parent_job_id: lineage.parent_job_id,
                initial_job_id: lineage.initial_job_id,
            },
        )?),
        JobEvent::JobCancelled(_) => Some(published(
            contract_jobs::evt_job_cancelled_v2_coords(),
            contract_jobs::event::EVENT_TYPE_CANCELLED,
            wire::EVENT_V2_SCHEMA_VERSION,
            &wire::JobCancelledV2 {
                job_id,
                parent_job_id: lineage.parent_job_id,
                initial_job_id: lineage.initial_job_id,
            },
        )?),
        JobEvent::JobFailed(fact) => {
            let cause = fact.failure_cause.as_db_str();
            let report = fact.report.as_ref();
            Some(published(
                contract_jobs::evt_job_failed_v2_coords(),
                contract_jobs::event::EVENT_TYPE_FAILED,
                wire::EVENT_V2_SCHEMA_VERSION,
                &wire::JobFailedV2 {
                    job_id,
                    parent_job_id: lineage.parent_job_id,
                    initial_job_id: lineage.initial_job_id,
                    requester: requester_ref(job.requester(), job.producer().as_str()),
                    failure_cause: cause.to_owned(),
                    failure_report: report.map(|report| FailureReport {
                        kind: report.kind().as_db_str().to_owned(),
                        reason_code: report.reason_code().as_str().to_owned(),
                        params: report.params().clone(),
                        diagnostic: report.diagnostic().clone(),
                    }),
                    run_id: fact.caused_by_run_id.map(|run| run.as_uuid()),
                    message: failure_message(
                        job_id,
                        cause,
                        report.map(|report| report.reason_code().as_str()),
                        report.is_some_and(|report| report.kind().is_retryable()),
                        fact.caused_by_run_id.map(|run| run.as_uuid()),
                    ),
                    note: note.map(str::to_owned),
                },
            )?)
        }
        _ => None,
    })
}

fn first_attempt(job: &Job, run_id: bc_jobs::domain::ids::RunId) -> bool {
    job.find_run(run_id)
        .ok()
        .is_some_and(|run| run.attempt_number().is_first())
}

fn published<T: Serialize>(
    coords: Result<br_core_integration::EventCoords, br_core_integration::CoordError>,
    event_type: &'static str,
    schema_version: u8,
    payload: &T,
) -> Result<Published, ServiceError> {
    Ok(Published {
        coords: coords.map_err(|error| ServiceError::Infra(error.to_string()))?,
        event_type,
        schema_version,
        payload: serde_json::to_value(payload)?,
    })
}
