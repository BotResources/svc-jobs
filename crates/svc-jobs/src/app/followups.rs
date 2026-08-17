use std::collections::{BTreeSet, HashMap};

use bc_jobs::domain::ids::JobId;
use bc_jobs::domain::job::Job;
use bc_jobs::event::job::JobEvent;
use bc_jobs::policies::affordances::{
    descendant_affordances_changed, predecessor_affordances_changed,
};
use sqlx::PgConnection;
use uuid::Uuid;

use super::write::JobChange;
use crate::db::{PgStore, hydrate};
use crate::error::ServiceError;

pub struct HeldRows {
    ids: BTreeSet<Uuid>,
    children: HashMap<Uuid, Vec<Uuid>>,
}

impl HeldRows {
    pub fn ids(&self) -> &BTreeSet<Uuid> {
        &self.ids
    }

    fn locked_children_of(&self, job_id: JobId) -> Vec<Uuid> {
        self.children
            .get(&job_id.as_uuid())
            .into_iter()
            .flatten()
            .copied()
            .filter(|id| self.ids.contains(id))
            .collect()
    }
}

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
) -> Result<HeldRows, ServiceError> {
    let mut ids: BTreeSet<Uuid> = changes
        .iter()
        .filter(|change| change.before.is_some())
        .map(|change| change.job_id.as_uuid())
        .collect();
    let mut children: HashMap<Uuid, Vec<Uuid>> = HashMap::new();
    for change in settling(changes) {
        if let Some(predecessor) = change
            .before
            .as_ref()
            .and_then(|before| before.predecessor_job_id())
        {
            ids.insert(predecessor.as_uuid());
        }
        let born = PgStore::child_ids(tx, change.job_id).await?;
        ids.extend(born.iter().copied());
        children.insert(change.job_id.as_uuid(), born);
    }
    Ok(HeldRows { ids, children })
}

pub async fn of_settled_jobs(
    tx: &mut PgConnection,
    changes: &[JobChange],
    held: &HeldRows,
) -> Result<Vec<JobChange>, ServiceError> {
    let mut wanted: BTreeSet<Uuid> = BTreeSet::new();
    for change in settling(changes) {
        wanted.insert(change.job_id.as_uuid());
        wanted.extend(held.locked_children_of(change.job_id));
    }
    if wanted.is_empty() {
        return Ok(Vec::new());
    }
    let wanted: Vec<Uuid> = wanted.into_iter().collect();
    let settled_state = hydrate::load_map(tx, &wanted).await?;
    let mut followups = Vec::new();
    for change in settling(changes) {
        let Some(settled) = settled_state.get(&change.job_id.as_uuid()) else {
            continue;
        };
        let children: Vec<Job> = held
            .locked_children_of(change.job_id)
            .into_iter()
            .filter_map(|id| settled_state.get(&id).cloned())
            .collect();
        let events = predecessor_affordances_changed(settled)
            .into_iter()
            .chain(descendant_affordances_changed(settled, &children));
        for event in events {
            followups.push(JobChange::new(event.job_id(), None, vec![event]));
        }
    }
    Ok(followups)
}
