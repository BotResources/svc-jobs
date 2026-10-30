use crate::commands::JobCommandResult;
use crate::domain::ids::{JobId, RunId};
use crate::domain::job::Job;
use crate::domain::job::parenting::{ParentContext, guard_admits_work};
use crate::domain::keys::ProducerKey;
use crate::domain::ownership::JobOwner;
use crate::domain::priority::{JobPriority, Priority, PriorityBasis};
use crate::error::JobsError;
use crate::event::job::JobEvent;
use crate::event::job_facts::JobQueued;

pub const RESTART_PRODUCER: &str = "jobs";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestartJob {
    pub restart_job_id: JobId,
    pub origin_run_id: RunId,
    pub applicable_maximum: Priority,
}

pub fn restart_job(
    origin: &Job,
    parent: ParentContext<'_>,
    command: RestartJob,
) -> Result<JobCommandResult, JobsError> {
    if command.restart_job_id == origin.id() {
        return Err(JobsError::SelfReference {
            field: "restart_job_id",
        });
    }
    origin.find_run(command.origin_run_id)?;
    let parent = parent.resolve(origin.parent_job_id())?;
    if let Some(parent) = parent {
        guard_admits_work(parent)?;
    }
    let basis = match parent {
        Some(parent) => PriorityBasis::inherited_from(parent),
        None => PriorityBasis::Capped {
            maximum: command.applicable_maximum,
        },
    };
    let priority = JobPriority::admitted(origin.priority().requested(), basis);
    let producer = ProducerKey::new(RESTART_PRODUCER)?;
    let owner = match origin.owner() {
        JobOwner::Producer(_) => JobOwner::Producer(producer.clone()),
        runner @ JobOwner::Runner { .. } => runner.clone(),
    };
    Ok(JobCommandResult::from_event(JobEvent::JobQueued(Box::new(
        JobQueued {
            job_id: command.restart_job_id,
            runner_type: origin.runner_type().clone(),
            producer,
            config: origin.config().cloned(),
            owner,
            parent_job_id: origin.parent_job_id(),
            predecessor_job_id: None,
            triggered_by: origin.triggered_by().cloned(),
            source: None,
            max_attempts: origin.max_attempts(),
            requested_priority: priority.requested(),
            effective_priority: priority.effective(),
            forced_priority: priority.forced(),
            initial_job_id: Some(origin.initial_job_id()),
            requester: Some(*origin.requester()),
            passport: origin.passport().cloned(),
            payload: origin.payload().cloned(),
            call: origin.call().copied(),
            restart_of_run_id: Some(command.origin_run_id),
            restart_of_job_id: Some(origin.id()),
        },
    ))))
}

#[cfg(test)]
mod tests;
