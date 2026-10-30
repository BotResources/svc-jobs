use bc_jobs::JobsError;
use bc_jobs::commands::job::resolution::{FailJob, FinishJob};
use bc_jobs::domain::ids::{JobId, ResolutionId};
use bc_jobs::domain::job::Job;
use bc_jobs::domain::ownership::{ActorRef, CancelRequester, ResolutionRequester};
use bc_jobs::domain::requester::{ActsFor, Caller, Requester};
use br_core_events::EventMetadata;
use contract_jobs::command_v3::{CancelJobV3, FailJobV3, FinishJobV3};
use contract_jobs::event_v2 as wire;
use serde_json::json;

use super::acting::{self, ActingRun};
use super::integration::{self, Lineage};
use super::write::{self, JobChange};
use super::{Jobs, resolve};
use crate::error::ServiceError;

pub async fn finish(
    jobs: &Jobs,
    wire: &FinishJobV3,
    metadata: &EventMetadata,
) -> Result<(), ServiceError> {
    let outcome = try_finish(jobs, wire, metadata).await;
    answered(
        jobs,
        wire::COMMAND_TYPE_REJECTED_FINISH,
        wire.job_id,
        metadata,
        outcome,
    )
    .await
}

pub async fn fail(
    jobs: &Jobs,
    wire: &FailJobV3,
    metadata: &EventMetadata,
) -> Result<(), ServiceError> {
    let outcome = try_fail(jobs, wire, metadata).await;
    answered(
        jobs,
        wire::COMMAND_TYPE_REJECTED_FAIL,
        wire.job_id,
        metadata,
        outcome,
    )
    .await
}

pub async fn cancel(
    jobs: &Jobs,
    wire: &CancelJobV3,
    metadata: &EventMetadata,
) -> Result<(), ServiceError> {
    let outcome = try_cancel(jobs, wire, metadata).await;
    answered(
        jobs,
        wire::COMMAND_TYPE_REJECTED_CANCEL,
        wire.job_id,
        metadata,
        outcome,
    )
    .await
}

async fn try_finish(
    jobs: &Jobs,
    wire: &FinishJobV3,
    metadata: &EventMetadata,
) -> Result<(), ServiceError> {
    let job = jobs.require(JobId::new(wire.job_id)?).await?;
    let requester = resolution_requester(jobs, &job, wire.acting(), metadata).await?;
    let result = job.finish(FinishJob {
        resolution_id: ResolutionId::new(jobs.ids.next())?,
        requester,
    })?;
    let id = job.id();
    jobs.commit(vec![JobChange::new(id, Some(job), result.events)], metadata)
        .await
}

async fn try_fail(
    jobs: &Jobs,
    wire: &FailJobV3,
    metadata: &EventMetadata,
) -> Result<(), ServiceError> {
    let job = jobs.require(JobId::new(wire.job_id)?).await?;
    let requester = resolution_requester(jobs, &job, wire.acting(), metadata).await?;
    let result = job.declare_failed(FailJob {
        resolution_id: ResolutionId::new(jobs.ids.next())?,
        requester,
    })?;
    let id = job.id();
    jobs.commit(
        vec![JobChange::new(id, Some(job), result.events).with_note(wire.note.clone())],
        metadata,
    )
    .await
}

async fn try_cancel(
    jobs: &Jobs,
    wire: &CancelJobV3,
    metadata: &EventMetadata,
) -> Result<(), ServiceError> {
    let job_id = JobId::new(wire.job_id)?;
    let job = jobs.require(job_id).await?;
    let acting = acting::named(
        jobs,
        wire.acting_run_id,
        wire.acting_instance_key.as_deref(),
    )
    .await?;
    let requester = match (job.requester(), &acting) {
        (Requester::Service(None), None) => {
            CancelRequester::Owner(resolve::owner_claim(jobs, &job, metadata).await?)
        }
        _ => {
            let (caller, acts_for) = caller_of(&job, acting, metadata);
            CancelRequester::Caller { caller, acts_for }
        }
    };
    let accepted = integration::cancel_accepted(wire.job_id, Lineage::of_job(&job))?;
    resolve::cancel_answered(
        jobs,
        job_id,
        ResolutionId::new(jobs.ids.next())?,
        requester,
        accepted,
        metadata,
    )
    .await
}

async fn resolution_requester(
    jobs: &Jobs,
    job: &Job,
    acting: (Option<uuid::Uuid>, Option<&str>),
    metadata: &EventMetadata,
) -> Result<ResolutionRequester, ServiceError> {
    let acting = acting::named(jobs, acting.0, acting.1).await?;
    if matches!(job.requester(), Requester::Service(None)) && acting.is_none() {
        return Ok(ResolutionRequester::LegacyOwner(
            resolve::owner_claim(jobs, job, metadata).await?,
        ));
    }
    let (caller, acts_for) = caller_of(job, acting, metadata);
    Ok(ResolutionRequester::Caller { caller, acts_for })
}

fn caller_of(job: &Job, acting: Option<ActingRun>, metadata: &EventMetadata) -> (Caller, ActsFor) {
    match acting {
        None => (
            Caller::Service(ActorRef::new(metadata.actor.id())),
            ActsFor::default(),
        ),
        Some(acting) => (
            Caller::Run {
                run_id: acting.run_id,
                works_the_job: job
                    .active_run()
                    .is_some_and(|run| run.id() == acting.run_id),
            },
            acting.acts_for,
        ),
    }
}

async fn answered(
    jobs: &Jobs,
    command_type: &str,
    job_id: uuid::Uuid,
    metadata: &EventMetadata,
    outcome: Result<(), ServiceError>,
) -> Result<(), ServiceError> {
    let reason = match &outcome {
        Err(ServiceError::Domain(refusal)) => refusal_code(refusal),
        Err(ServiceError::JobNotFound) => wire::REASON_JOB_UNKNOWN,
        _ => return outcome,
    };
    let params = match &outcome {
        Err(ServiceError::Domain(refusal)) => refusal.params(),
        _ => json!({}),
    };
    let lineage = match JobId::new(job_id) {
        Ok(id) => jobs.load(id).await?.as_ref().map(Lineage::of_job),
        Err(_) => None,
    };
    let rejection = integration::command_rejected(command_type, job_id, lineage, reason, params)?;
    write::publish_command_rejection(
        &jobs.store,
        jobs.ids.as_ref(),
        rejection,
        metadata,
        jobs.clock.now(),
    )
    .await
}

fn refusal_code(refusal: &JobsError) -> &'static str {
    match refusal {
        JobsError::JobAlreadyTerminal { .. } => wire::REASON_JOB_TERMINAL,
        JobsError::JobDeleted => wire::REASON_JOB_UNKNOWN,
        other => other.code(),
    }
}
