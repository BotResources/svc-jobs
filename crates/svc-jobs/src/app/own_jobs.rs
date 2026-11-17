//! The user surface over Jobs: the Jobs that run for the caller, and their
//! cancel. Which Job is the caller's is the domain's decision
//! (`PrincipalFacts::guard_job_runs_for_caller`, `guard_cancels_job`).

use bc_jobs::domain::access::PrincipalFacts;
use bc_jobs::domain::ids::{JobId, ResolutionId};
use bc_jobs::domain::job::Job;
use bc_jobs::domain::ownership::CancelRequester;
use bc_jobs::domain::references::KnownUser;
use uuid::Uuid;

use super::admin::administrator_metadata;
use super::{Jobs, resolve};
use crate::error::ServiceError;

/// A Job of the caller. A Job that does not run for them, or that an
/// administrator soft-deleted, is unknown to them, like a Job that does not
/// exist.
pub async fn own_job(
    jobs: &Jobs,
    principal: &PrincipalFacts,
    job_id: JobId,
) -> Result<Job, ServiceError> {
    match jobs.load(job_id).await? {
        Some(job)
            if !job.is_deleted()
                && principal
                    .guard_job_runs_for_caller(job.triggered_by())
                    .is_ok() =>
        {
            Ok(job)
        }
        _ => Err(ServiceError::JobNotFound),
    }
}

/// The cancel of a Job by the user it runs for: who may cancel is judged
/// before the Job's state, by the same decision as the cancel affordance.
pub async fn cancel(
    jobs: &Jobs,
    principal: &PrincipalFacts,
    user: KnownUser,
    resolution_id: Uuid,
    job_id: Uuid,
) -> Result<(), ServiceError> {
    let job_id = JobId::new(job_id)?;
    let job = jobs.require(job_id).await?;
    principal.guard_cancels_job(job.triggered_by())?;
    let metadata = administrator_metadata(&user);
    resolve::cancel(
        jobs,
        job_id,
        ResolutionId::new(resolution_id)?,
        CancelRequester::User(user),
        &metadata,
    )
    .await
}
