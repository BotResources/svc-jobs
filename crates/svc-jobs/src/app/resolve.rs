use bc_jobs::commands::job::cancellation::CancelJob;
use bc_jobs::commands::job::resolution::{FailJob, FinishJob};
use bc_jobs::domain::ids::{JobId, ResolutionId};
use bc_jobs::domain::job::Job;
use bc_jobs::domain::ownership::{
    ActorRef, CancelRequester, DeclarationClaim, ResolutionRequester,
};
use bc_jobs::policies::cascade::cancellation_cascade;
use bc_jobs::ports::job::JobReader;
use br_core_events::EventMetadata;

use super::Jobs;
use super::write::JobChange;
use crate::error::ServiceError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntegrationAdmission {
    LegacyOwner,
    LifecycleOnly,
}

pub async fn owner_claim(
    jobs: &Jobs,
    job: &Job,
    metadata: &EventMetadata,
) -> Result<DeclarationClaim, ServiceError> {
    let mut governing = job.clone();
    while let Some(predecessor) = governing.predecessor_job_id() {
        match jobs.load(predecessor).await? {
            Some(earlier) => governing = earlier,
            None => break,
        }
    }
    let declared_by = jobs.store.declaring_actor(governing.id()).await?;
    Ok(DeclarationClaim::new(
        declared_by.map(ActorRef::new),
        ActorRef::new(metadata.actor.id()),
    ))
}

async fn resolution_requester(
    jobs: &Jobs,
    job: &Job,
    metadata: &EventMetadata,
    admission: IntegrationAdmission,
) -> Result<ResolutionRequester, ServiceError> {
    match admission {
        IntegrationAdmission::LegacyOwner => Ok(ResolutionRequester::LegacyOwner(
            owner_claim(jobs, job, metadata).await?,
        )),
        IntegrationAdmission::LifecycleOnly => Ok(ResolutionRequester::Declarant),
    }
}

pub async fn finish(
    jobs: &Jobs,
    job_id: JobId,
    resolution_id: ResolutionId,
    admission: IntegrationAdmission,
    metadata: &EventMetadata,
) -> Result<(), ServiceError> {
    let job = jobs.require(job_id).await?;
    let requester = resolution_requester(jobs, &job, metadata, admission).await?;
    let result = job.finish(FinishJob {
        resolution_id,
        requester,
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
    admission: IntegrationAdmission,
    metadata: &EventMetadata,
) -> Result<(), ServiceError> {
    let job = jobs.require(job_id).await?;
    let requester = resolution_requester(jobs, &job, metadata, admission).await?;
    let result = job.declare_failed(FailJob {
        resolution_id,
        requester,
    })?;
    jobs.commit(
        vec![JobChange::new(job_id, Some(job), result.events).with_note(note)],
        metadata,
    )
    .await
}

pub async fn cancel_from_integration(
    jobs: &Jobs,
    job_id: JobId,
    resolution_id: ResolutionId,
    admission: IntegrationAdmission,
    metadata: &EventMetadata,
) -> Result<(), ServiceError> {
    let job = jobs.require(job_id).await?;
    let requester = match admission {
        IntegrationAdmission::LegacyOwner => {
            CancelRequester::Owner(owner_claim(jobs, &job, metadata).await?)
        }
        IntegrationAdmission::LifecycleOnly => CancelRequester::Declarant,
    };
    cancel(jobs, job_id, resolution_id, requester, metadata).await
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
    let mut cascaded: Vec<JobChange> = Vec::new();
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
        cascaded.push(JobChange::new(
            order.job_id,
            Some(descendant.clone()),
            cascade.events,
        ));
    }
    cascaded.sort_by_key(|change| change.job_id.as_uuid());
    let mut changes = vec![JobChange::new(job_id, Some(job.clone()), result.events)];
    changes.append(&mut cascaded);
    jobs.commit(changes, metadata).await
}
