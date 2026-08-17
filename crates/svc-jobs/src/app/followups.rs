use std::collections::BTreeSet;

use bc_jobs::domain::ids::JobId;
use bc_jobs::domain::job::Job;
use bc_jobs::event::job::JobEvent;
use bc_jobs::policies::affordances::{
    descendant_affordances_changed, predecessor_affordances_changed,
};
use sqlx::PgConnection;
use uuid::Uuid;

use super::write::JobChange;
use crate::db::PgStore;
use crate::error::ServiceError;

pub fn settles_job(event: &JobEvent) -> bool {
    matches!(
        event,
        JobEvent::JobCompleted(_) | JobEvent::JobFailed(_) | JobEvent::JobCancelled(_)
    )
}

fn settling(changes: &[JobChange]) -> impl Iterator<Item = &JobChange> {
    changes
        .iter()
        .filter(|change| change.events.iter().any(settles_job))
}

pub async fn rows_to_lock(
    tx: &mut PgConnection,
    changes: &[JobChange],
) -> Result<BTreeSet<Uuid>, ServiceError> {
    let mut rows: BTreeSet<Uuid> = changes
        .iter()
        .filter(|change| change.before.is_some())
        .map(|change| change.job_id.as_uuid())
        .collect();
    for change in settling(changes) {
        if let Some(predecessor) = change
            .before
            .as_ref()
            .and_then(|before| before.predecessor_job_id())
        {
            rows.insert(predecessor.as_uuid());
        }
        rows.extend(PgStore::child_ids(tx, change.job_id).await?);
    }
    Ok(rows)
}

pub async fn of_settled_jobs(
    tx: &mut PgConnection,
    changes: &[JobChange],
    held: &BTreeSet<Uuid>,
) -> Result<Vec<JobChange>, ServiceError> {
    let mut followups = Vec::new();
    for change in settling(changes) {
        let Some(settled) = PgStore::lock_job(tx, change.job_id).await? else {
            continue;
        };
        let children = children_of(tx, change.job_id, held).await?;
        let events = predecessor_affordances_changed(&settled)
            .into_iter()
            .chain(descendant_affordances_changed(&settled, &children));
        for event in events {
            followups.push(JobChange::new(event.job_id(), None, vec![event]));
        }
    }
    Ok(followups)
}

async fn children_of(
    tx: &mut PgConnection,
    job_id: JobId,
    held: &BTreeSet<Uuid>,
) -> Result<Vec<Job>, ServiceError> {
    let mut children = Vec::new();
    for id in PgStore::child_ids(tx, job_id).await? {
        if !held.contains(&id) {
            continue;
        }
        if let Some(child) = PgStore::lock_job(tx, JobId::new(id)?).await? {
            children.push(child);
        }
    }
    Ok(children)
}
