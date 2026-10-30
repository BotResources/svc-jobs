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
use super::integration::Published;
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
const CANCEL_ATTEMPTS: usize = 5;

pub async fn cancel(
    jobs: &Jobs,
    job_id: JobId,
    resolution_id: ResolutionId,
    requester: CancelRequester,
    metadata: &EventMetadata,
) -> Result<(), ServiceError> {
    cancel_with(jobs, job_id, resolution_id, requester, None, metadata).await
}

pub async fn cancel_answered(
    jobs: &Jobs,
    job_id: JobId,
    resolution_id: ResolutionId,
    requester: CancelRequester,
    accepted: Published,
    metadata: &EventMetadata,
) -> Result<(), ServiceError> {
    cancel_with(
        jobs,
        job_id,
        resolution_id,
        requester,
        Some(accepted),
        metadata,
    )
    .await
}

async fn cancel_with(
    jobs: &Jobs,
    job_id: JobId,
    resolution_id: ResolutionId,
    requester: CancelRequester,
    accepted: Option<Published>,
    metadata: &EventMetadata,
) -> Result<(), ServiceError> {
    let mut outcome = Err(ServiceError::Contended);
    for _ in 0..CANCEL_ATTEMPTS {
        outcome = cancel_once(
            jobs,
            job_id,
            resolution_id,
            requester.clone(),
            accepted.clone(),
            metadata,
        )
        .await;
        if !matches!(outcome, Err(ServiceError::Contended)) {
            return outcome;
        }
    }
    outcome
}

async fn cancel_once(
    jobs: &Jobs,
    job_id: JobId,
    resolution_id: ResolutionId,
    requester: CancelRequester,
    accepted: Option<Published>,
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
        cascaded.push(
            JobChange::new(order.job_id, Some(descendant.clone()), cascade.events)
                .with_metadata(originated_by_jobs(jobs, metadata)),
        );
    }
    cascaded.sort_by_key(|change| change.job_id.as_uuid());
    let mut root = JobChange::new(job_id, Some(job.clone()), result.events)
        .over_descendants(descendants.iter().map(Job::id));
    if let Some(accepted) = accepted {
        root = root.answering(accepted);
    }
    let mut changes = vec![root];
    changes.append(&mut cascaded);
    jobs.commit(changes, metadata).await
}

fn originated_by_jobs(jobs: &Jobs, metadata: &EventMetadata) -> EventMetadata {
    let own = EventMetadata::new(metadata.actor, jobs.ids.next());
    match metadata.causation_id {
        Some(causation) => own.with_causation(causation),
        None => own,
    }
}
