use std::collections::HashMap;

use chrono::{DateTime, Utc};

use crate::domain::actions::Availability;
use crate::domain::ids::JobId;
use crate::domain::job::Job;
use crate::domain::job::parenting::ParentContext;
use crate::event::job::JobEvent;
use crate::event::job_facts::{JobAffordancesChanged, JobContentErased};

pub fn events_of_a_scrub(tree: &[Job], at: DateTime<Utc>) -> Vec<JobEvent> {
    let by_id: HashMap<JobId, &Job> = tree.iter().map(|job| (job.id(), job)).collect();
    let mut events = Vec::new();
    for job in tree {
        events.push(JobEvent::JobContentErased(JobContentErased {
            job_id: job.id(),
            at: job.content_erased_at().unwrap_or(at),
        }));
        if loses_its_manual_retry(job, &by_id) {
            events.push(JobEvent::JobAffordancesChanged(JobAffordancesChanged {
                job_id: job.id(),
                caused_by_job_id: None,
            }));
        }
    }
    events
}

fn loses_its_manual_retry(job: &Job, tree: &HashMap<JobId, &Job>) -> bool {
    let parent = job.parent_job_id().and_then(|id| tree.get(&id).copied());
    let before = job.can_manual_retry(ParentContext::of(parent));
    matches!(before, Ok(Availability::Available))
}

#[cfg(test)]
mod tests;
