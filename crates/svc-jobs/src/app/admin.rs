use bc_jobs::commands::job::deletion::DeleteJob;
use bc_jobs::commands::job::manual_retry::{
    ManualRetryJob, ManualRetryOutcome, SuccessorDispatch, manual_retry as decide_manual_retry,
};
use bc_jobs::domain::fleet::dispatch_mode::DispatchMode;
use bc_jobs::domain::fleet::run_duration::MaxRunDuration;
use bc_jobs::domain::ids::{JobId, KnownUserId, ManualRetryId, ResolutionId, RunId};
use bc_jobs::domain::job::parenting::ParentContext;
use bc_jobs::domain::ownership::CancelRequester;
use bc_jobs::domain::priority::PriorityBasis;
use bc_jobs::domain::references::KnownUser;
use bc_jobs::ports::fleet::FleetReader;
use bc_jobs::ports::job::JobReader;
use br_core_events::{Actor, EventMetadata};
use uuid::Uuid;

use super::write::{JobChange, PriorityPremise};
use super::{Jobs, resolve};
use crate::error::ServiceError;

pub fn administrator_metadata(actor: &KnownUser) -> EventMetadata {
    EventMetadata::new(Actor::Human(actor.id()), Uuid::now_v7())
}

pub async fn cancel(
    jobs: &Jobs,
    actor: KnownUser,
    resolution_id: Uuid,
    job_id: Uuid,
) -> Result<(), ServiceError> {
    let metadata = administrator_metadata(&actor);
    resolve::cancel(
        jobs,
        JobId::new(job_id)?,
        ResolutionId::new(resolution_id)?,
        CancelRequester::Administrator(actor),
        &metadata,
    )
    .await
}

pub async fn delete(jobs: &Jobs, actor: KnownUser, job_id: Uuid) -> Result<(), ServiceError> {
    let metadata = administrator_metadata(&actor);
    let job_id = JobId::new(job_id)?;
    let job = jobs.require(job_id).await?;
    let result = job.delete(DeleteJob { deleted_by: actor })?;
    jobs.commit(
        vec![JobChange::new(job_id, Some(job), result.events)],
        &metadata,
    )
    .await
}

pub struct ManualRetryInput {
    pub manual_retry_id: Uuid,
    pub job_id: Uuid,
    pub successor_job_id: Uuid,
    pub failed_resolution_id: Uuid,
}

pub async fn manual_retry(
    jobs: &Jobs,
    actor: KnownUser,
    input: ManualRetryInput,
) -> Result<(), ServiceError> {
    let metadata = administrator_metadata(&actor);
    let predecessor = jobs.require(JobId::new(input.job_id)?).await?;
    let parent = match predecessor.parent_job_id() {
        Some(parent) => jobs.load(parent).await?,
        None => None,
    };
    let runner_type = FleetReader::load(&jobs.store, predecessor.runner_type()).await?;
    let dispatch = match &runner_type {
        Some(known) if known.dispatch_mode() == DispatchMode::Claim => {
            SuccessorDispatch::AwaitClaim
        }
        known => SuccessorDispatch::Push {
            first_run_id: RunId::new(jobs.ids.next())?,
            max_run_duration: known
                .as_ref()
                .map(|known| known.max_run_duration())
                .map_or_else(
                    || MaxRunDuration::from_delta(jobs.limits.max_run_duration()),
                    Ok,
                )?,
        },
    };
    let user_maximum = match predecessor.triggered_by() {
        Some(user) => Some(
            jobs.store
                .user_max_priority(KnownUserId::new(user.id().0)?)
                .await?,
        ),
        None => None,
    };
    let command = ManualRetryJob {
        manual_retry_id: ManualRetryId::new(input.manual_retry_id)?,
        failed_resolution_id: ResolutionId::new(input.failed_resolution_id)?,
        successor_job_id: JobId::new(input.successor_job_id)?,
        dispatch,
        requested_by: actor,
        applicable_maximum: user_maximum.as_ref().map_or_else(
            || jobs.limits.default_max_priority(),
            |user| user.applicable(&jobs.limits),
        ),
    };
    let admitted_under = predecessor.parent_job_id().and(parent.as_ref());
    let premise = PriorityPremise::of(
        PriorityBasis::of(admitted_under, user_maximum.as_ref(), &jobs.limits),
        admitted_under,
        user_maximum.as_ref(),
    );
    let claimed_source = predecessor.source();
    let active = match &claimed_source {
        Some(source) => JobReader::load_active_for_source(&jobs.store, source).await?,
        None => None,
    };
    let outcome = decide_manual_retry(
        &predecessor,
        ParentContext::of(parent.as_ref()),
        active.as_ref(),
        command.clone(),
    )?;
    match outcome {
        ManualRetryOutcome::AlreadyStarted => Ok(()),
        ManualRetryOutcome::Started(plan) => {
            jobs.commit(
                vec![
                    JobChange::new(command.successor_job_id, None, plan.successor.events)
                        .claiming_source(claimed_source)
                        .on_priority_premise(premise),
                    JobChange::new(predecessor.id(), Some(predecessor), plan.predecessor.events),
                ],
                &metadata,
            )
            .await
        }
    }
}
