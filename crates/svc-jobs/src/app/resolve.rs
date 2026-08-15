use bc_jobs::commands::job::cancellation::CancelJob;
use bc_jobs::commands::job::resolution::{FailJob, FinishJob};
use bc_jobs::domain::ids::{JobId, ResolutionId};
use bc_jobs::domain::job::Job;
use bc_jobs::domain::ownership::{Caller, CancelRequester, JobOwner};
use bc_jobs::policies::cascade::cancellation_cascade;
use bc_jobs::ports::job::JobReader;
use br_core_events::EventMetadata;

use super::Jobs;
use super::write::JobChange;
use crate::error::ServiceError;

pub async fn guard_declaring_service(
    jobs: &Jobs,
    job: &Job,
    metadata: &EventMetadata,
) -> Result<(), ServiceError> {
    let mut bound = job.clone();
    while let Some(predecessor) = bound.predecessor_job_id() {
        match jobs.load(predecessor).await? {
            Some(earlier) => bound = earlier,
            None => break,
        }
    }
    match jobs.store.declaring_actor(bound.id()).await? {
        Some(declared_by) if declared_by == metadata.actor.id() => Ok(()),
        _ => Err(ServiceError::Domain(bc_jobs::JobsError::NotOwner)),
    }
}

pub fn caller_of(job: &Job) -> Caller {
    match job.owner() {
        JobOwner::Producer(producer) => Caller::Producer(producer.clone()),
        JobOwner::Runner {
            parent_job_id,
            runner_type,
        } => Caller::Runner {
            runner_type: runner_type.clone(),
            executing_job_id: *parent_job_id,
        },
    }
}

pub async fn finish(
    jobs: &Jobs,
    job_id: JobId,
    resolution_id: ResolutionId,
    metadata: &EventMetadata,
) -> Result<(), ServiceError> {
    let job = jobs.require(job_id).await?;
    guard_declaring_service(jobs, &job, metadata).await?;
    let caller = caller_of(&job);
    let result = job.finish(FinishJob {
        resolution_id,
        caller,
    })?;
    jobs.commit(
        vec![JobChange::new(job_id, Some(job), result.events)],
        metadata,
    )
    .await
}

pub async fn fail(
    jobs: &Jobs,
    job_id: JobId,
    resolution_id: ResolutionId,
    note: Option<String>,
    metadata: &EventMetadata,
) -> Result<(), ServiceError> {
    let job = jobs.require(job_id).await?;
    guard_declaring_service(jobs, &job, metadata).await?;
    let caller = caller_of(&job);
    let result = job.declare_failed(FailJob {
        resolution_id,
        caller,
    })?;
    jobs.commit(
        vec![JobChange::new(job_id, Some(job), result.events).with_note(note)],
        metadata,
    )
    .await
}

pub async fn cancel(
    jobs: &Jobs,
    job_id: JobId,
    resolution_id: ResolutionId,
    requester: CancelRequester,
    metadata: &EventMetadata,
) -> Result<(), ServiceError> {
    let job = jobs.require(job_id).await?;
    let result = job.cancel(CancelJob {
        resolution_id,
        requester,
    })?;
    let descendants = JobReader::load_descendants(&jobs.store, job_id).await?;
    let mut changes = vec![JobChange::new(job_id, Some(job.clone()), result.events)];
    for order in cancellation_cascade(&job, &descendants) {
        let Some(descendant) = descendants
            .iter()
            .find(|candidate| candidate.id() == order.job_id)
        else {
            continue;
        };
        let cascade = descendant.cancel(CancelJob {
            resolution_id: ResolutionId::new(jobs.ids.next())?,
            requester: CancelRequester::Cascade {
                originating_job_id: order.originating_job_id,
            },
        })?;
        changes.push(JobChange::new(
            order.job_id,
            Some(descendant.clone()),
            cascade.events,
        ));
    }
    changes.sort_by_key(|change| change.job_id.as_uuid());
    jobs.commit(changes, metadata).await
}
