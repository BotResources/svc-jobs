//! Priority gestures: a super user forcing a Job, or setting and clearing the
//! maximum of a user, and a user reading their own maximum.

use bc_jobs::commands::UserCommandResult;
use bc_jobs::commands::job::priority::ForcePriority;
use bc_jobs::commands::user::{
    ClearMaxPriority, SetMaxPriority, clear_max_priority, set_max_priority,
};
use bc_jobs::domain::access::PrincipalFacts;
use bc_jobs::domain::ids::{JobId, KnownUserId};
use bc_jobs::domain::priority::{Priority, UserMaxPriority};
use bc_jobs::domain::references::KnownUser;
use bc_jobs::policies::priority::{forcing_cascade, recalculate_for_user};
use bc_jobs::ports::job::JobReader;
use uuid::Uuid;

use super::Jobs;
use super::admin::administrator_metadata;
use super::write::{self, JobChange, UserChange};
use crate::error::ServiceError;

/// A concurrent write moving one of the Jobs a gesture reaches is retried
/// against fresh state this many times before the gesture reports a conflict.
const ATTEMPTS: usize = 5;

pub async fn force(
    jobs: &Jobs,
    actor: KnownUser,
    principal: &PrincipalFacts,
    job_id: Uuid,
    priority: i64,
) -> Result<(), ServiceError> {
    let job_id = JobId::new(job_id)?;
    let priority = Priority::new(priority)?;
    let metadata = administrator_metadata(&actor);
    let mut outcome = Err(ServiceError::Contended);
    for _ in 0..ATTEMPTS {
        let job = jobs.require(job_id).await?;
        let command = ForcePriority {
            priority,
            forced_by: actor.clone(),
        };
        let result = job.force_priority(command.clone(), principal)?;
        let descendants = JobReader::load_descendants(&jobs.store, job_id).await?;
        let mut changes = vec![JobChange::new(job_id, Some(job.clone()), result.events)];
        for order in forcing_cascade(&job, &descendants) {
            let Some(descendant) = descendants.iter().find(|found| found.id() == order.job_id)
            else {
                continue;
            };
            let cascaded =
                descendant.force_priority_as_descendant(command.clone(), order.originating_job_id);
            changes.push(JobChange::new(
                order.job_id,
                Some(descendant.clone()),
                cascaded.events,
            ));
        }
        outcome = jobs.commit(changes, &metadata).await;
        if !matches!(outcome, Err(ServiceError::Contended)) {
            return outcome;
        }
    }
    outcome
}

pub async fn set_max(
    jobs: &Jobs,
    actor: KnownUser,
    principal: &PrincipalFacts,
    user_id: Uuid,
    max_priority: i64,
) -> Result<(), ServiceError> {
    let max_priority = Priority::new(max_priority)?;
    let set_by = actor.clone();
    change_max(jobs, actor, KnownUserId::new(user_id)?, |current| {
        set_max_priority(
            current,
            SetMaxPriority {
                max_priority,
                set_by: set_by.clone(),
            },
            principal,
        )
    })
    .await
}

pub async fn clear_max(
    jobs: &Jobs,
    actor: KnownUser,
    principal: &PrincipalFacts,
    user_id: Uuid,
) -> Result<(), ServiceError> {
    let cleared_by = actor.clone();
    change_max(jobs, actor, KnownUserId::new(user_id)?, |current| {
        clear_max_priority(
            current,
            ClearMaxPriority {
                cleared_by: cleared_by.clone(),
            },
            principal,
        )
    })
    .await
}

/// The maximum that applies to a user: their own, or the service default.
pub async fn applicable_max(jobs: &Jobs, user_id: Uuid) -> Result<Priority, ServiceError> {
    let user = jobs
        .store
        .user_max_priority(KnownUserId::new(user_id)?)
        .await?;
    Ok(user.applicable(&jobs.limits))
}

async fn change_max<F>(
    jobs: &Jobs,
    actor: KnownUser,
    user_id: KnownUserId,
    decide: F,
) -> Result<(), ServiceError>
where
    F: Fn(&UserMaxPriority) -> Result<UserCommandResult, bc_jobs::JobsError>,
{
    let metadata = administrator_metadata(&actor);
    let mut outcome = Err(ServiceError::Contended);
    for _ in 0..ATTEMPTS {
        let current = jobs.store.user_max_priority(user_id).await?;
        let result = decide(&current)?;
        let Some(resulting) = result.events.last().map(|event| event.resulting_maximum()) else {
            return Ok(());
        };
        let (roots, descendants) = jobs.store.priority_reach_of(user_id).await?;
        let recalculations = recalculate_for_user(
            &UserMaxPriority::new(user_id, resulting),
            &roots,
            &descendants,
            &jobs.limits,
        )
        .into_iter()
        .map(|recalculation| {
            let before = roots
                .iter()
                .chain(&descendants)
                .find(|job| job.id() == recalculation.job_id)
                .cloned();
            JobChange::new(recalculation.job_id, before, recalculation.result.events)
        })
        .collect();
        outcome = write::commit_user_change(
            &jobs.store,
            jobs.ids.as_ref(),
            UserChange {
                user_id,
                decided_on: current.maximum(),
                events: result.events,
                recalculations,
            },
            &metadata,
            jobs.clock.now(),
        )
        .await;
        if !matches!(outcome, Err(ServiceError::Contended)) {
            return outcome;
        }
    }
    outcome
}
