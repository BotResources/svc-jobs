use bc_jobs::JobsError;
use bc_jobs::commands::job::restart::{RestartJob, restart_job};
use bc_jobs::domain::ids::{JobId, KnownUserId, RunId};
use bc_jobs::domain::job::parenting::ParentContext;
use bc_jobs::domain::priority::PriorityBasis;
use bc_jobs::ports::user::UserMaxPriorityReader;
use uuid::Uuid;

use super::write::{JobChange, PriorityPremise};
use super::{Jobs, service_metadata};
use crate::error::ServiceError;

pub async fn process_wakes(jobs: &Jobs) -> Result<(), ServiceError> {
    for origin_run in jobs.store.due_wakes().await? {
        restart_run(jobs, origin_run).await?;
    }
    Ok(())
}

async fn restart_run(jobs: &Jobs, origin_run: Uuid) -> Result<(), ServiceError> {
    let Some(origin_job_id) = jobs.store.job_of_run(origin_run).await? else {
        return Ok(jobs.store.drop_wake(origin_run).await?);
    };
    let origin = jobs.require(origin_job_id).await?;
    let parent = match origin.parent_job_id() {
        Some(parent) => jobs.load(parent).await?,
        None => None,
    };
    let user_maximum = match origin.triggered_by() {
        Some(user) => Some(
            UserMaxPriorityReader::load_one(&jobs.store, KnownUserId::new(user.id().0)?).await?,
        ),
        None => None,
    };
    let origin_run_id = RunId::new(origin_run)?;
    let restart_job_id = JobId::new(jobs.ids.next())?;
    let command = RestartJob {
        restart_job_id,
        origin_run_id,
        applicable_maximum: user_maximum.as_ref().map_or_else(
            || jobs.limits.default_max_priority(),
            |user| user.applicable(&jobs.limits),
        ),
    };
    let result = match restart_job(&origin, ParentContext::of(parent.as_ref()), command) {
        Ok(result) => result,
        Err(refusal) => return refused(jobs, origin_run, &refusal).await,
    };
    let admitted_under = origin.parent_job_id().and(parent.as_ref());
    let premise = PriorityPremise::of(
        PriorityBasis::of(admitted_under, user_maximum.as_ref(), &jobs.limits),
        admitted_under,
        user_maximum.as_ref(),
    );
    let change = JobChange::new(restart_job_id, None, result.events)
        .restarting(origin_run, origin.initial_job_id())
        .on_priority_premise(premise);
    match jobs.commit(vec![change], &service_metadata()).await {
        Err(ServiceError::RestartNotDue) => Ok(jobs.store.drop_wake(origin_run).await?),
        Err(ServiceError::Domain(refusal)) => refused(jobs, origin_run, &refusal).await,
        settled => settled,
    }
}

async fn refused(jobs: &Jobs, origin_run: Uuid, refusal: &JobsError) -> Result<(), ServiceError> {
    match refusal {
        JobsError::RunnerTypeRetired { .. }
        | JobsError::ParentJobTerminal { .. }
        | JobsError::ParentJobDeleted { .. }
        | JobsError::ParentJobUnknown { .. } => {
            tracing::info!(
                run_id = %origin_run,
                code = refusal.code(),
                "a restart was refused; the mail or notice stays unread"
            );
            Ok(jobs.store.drop_wake(origin_run).await?)
        }
        other => Err(other.clone().into()),
    }
}
