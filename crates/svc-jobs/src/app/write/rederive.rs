use std::collections::HashMap;

use bc_jobs::domain::ids::KnownUserId;
use bc_jobs::domain::job::Job;
use bc_jobs::domain::policy::ServiceLimits;
use bc_jobs::domain::priority::{PriorityBasis, UserMaxPriority};
use bc_jobs::event::job::JobEvent;
use uuid::Uuid;

use super::change::JobChange;
use crate::db::{PgStore, hydrate};
use crate::error::ServiceError;

fn returns_to_waiting(event: &JobEvent) -> bool {
    match event {
        JobEvent::RetryScheduled(_) => true,
        JobEvent::RunCancelled(fact) => fact.withdrawn_for.is_some(),
        _ => false,
    }
}

pub async fn of_jobs_back_to_waiting(
    tx: &mut sqlx::PgConnection,
    changes: &[JobChange],
    limits: &ServiceLimits,
) -> Result<Vec<JobChange>, ServiceError> {
    let mut returning: Vec<Uuid> = changes
        .iter()
        .filter(|change| change.events.iter().any(returns_to_waiting))
        .map(|change| change.job_id.as_uuid())
        .collect();
    if returning.is_empty() {
        return Ok(Vec::new());
    }
    returning.sort_unstable();
    returning.dedup();
    let waiting = hydrate::load_map(tx, &returning).await?;
    let parent_ids: Vec<Uuid> = waiting
        .values()
        .filter_map(Job::parent_job_id)
        .map(|id| id.as_uuid())
        .collect();
    let parents: HashMap<Uuid, Job> = if parent_ids.is_empty() {
        HashMap::new()
    } else {
        hydrate::load_map(tx, &parent_ids).await?
    };
    let mut followups = Vec::new();
    for job_id in returning {
        let Some(job) = waiting.get(&job_id) else {
            continue;
        };
        if !job.is_waiting() || job.priority().is_forced() {
            continue;
        }
        let parent = job
            .parent_job_id()
            .and_then(|parent| parents.get(&parent.as_uuid()));
        let user = match (parent, job.triggered_by()) {
            (None, Some(known)) => {
                let user_id = KnownUserId::new(known.id().0)?;
                let maximum = PgStore::user_max_priority_in(tx, user_id).await?;
                Some(UserMaxPriority::new(user_id, maximum))
            }
            _ => None,
        };
        if job.parent_job_id().is_some() && parent.is_none() {
            continue;
        }
        let result = job.rederive_priority(PriorityBasis::of(parent, user.as_ref(), limits));
        if !result.events.is_empty() {
            followups.push(JobChange::new(job.id(), None, result.events));
        }
    }
    Ok(followups)
}
