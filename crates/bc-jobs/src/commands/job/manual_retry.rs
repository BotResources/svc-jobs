use crate::commands::job::source::guard_source_free;
use crate::commands::{CommandResult, JobCommandResult};
use crate::domain::attempts::AttemptNumber;
use crate::domain::ids::{JobId, ManualRetryId, ResolutionId};
use crate::domain::job::Job;
use crate::domain::job::parenting::ParentContext;
use crate::domain::priority::{JobPriority, Priority, PriorityBasis};
use crate::domain::references::KnownUser;
use crate::domain::run::origin::RunOrigin;
use crate::error::JobsError;
use crate::event::job::JobEvent;
use crate::event::job_facts::{JobQueued, ManualRetryStarted, RunDispatched};

pub use crate::domain::fleet::dispatch_mode::SuccessorDispatch;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManualRetryJob {
    pub manual_retry_id: ManualRetryId,
    pub failed_resolution_id: ResolutionId,
    pub successor_job_id: JobId,
    pub dispatch: SuccessorDispatch,
    pub requested_by: KnownUser,
    /// The maximum that applies to the user the successor runs for — theirs or
    /// the service default — which caps the request it copies when it has no
    /// parent.
    pub applicable_maximum: Priority,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManualRetryPlan {
    pub predecessor: JobCommandResult,
    pub successor: JobCommandResult,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManualRetryOutcome {
    Started(Box<ManualRetryPlan>),
    AlreadyStarted,
}

pub fn manual_retry(
    predecessor: &Job,
    parent: ParentContext<'_>,
    active_for_source: Option<&Job>,
    command: ManualRetryJob,
) -> Result<ManualRetryOutcome, JobsError> {
    if let Some(record) = predecessor.manual_retry() {
        if record.matches(command.manual_retry_id, command.successor_job_id) {
            return Ok(ManualRetryOutcome::AlreadyStarted);
        }
        if record.id() == command.manual_retry_id {
            return Err(JobsError::ManualRetryConflict {
                successor_job_id: record.successor_job_id().as_uuid(),
            });
        }
    }
    predecessor.guard_manual_retry(parent)?;
    if command.successor_job_id == predecessor.id() {
        return Err(JobsError::SelfReference {
            field: "successor_job_id",
        });
    }
    guard_current_failure(predecessor, command.failed_resolution_id)?;
    guard_source_free(predecessor.source(), active_for_source)?;
    let basis = match parent.resolve(predecessor.parent_job_id())? {
        Some(parent) => PriorityBasis::inherited_from(parent),
        None => PriorityBasis::Capped {
            maximum: command.applicable_maximum,
        },
    };
    let priority = JobPriority::admitted(predecessor.priority().requested(), basis);
    let mut successor = vec![JobEvent::JobQueued(Box::new(JobQueued {
        job_id: command.successor_job_id,
        runner_type: predecessor.runner_type().clone(),
        producer: predecessor.producer().clone(),
        config: predecessor.config().cloned(),
        owner: predecessor.owner().clone(),
        parent_job_id: predecessor.parent_job_id(),
        predecessor_job_id: Some(predecessor.id()),
        triggered_by: predecessor.triggered_by().cloned(),
        source: predecessor.source(),
        max_attempts: predecessor.max_attempts(),
        requested_priority: priority.requested(),
        effective_priority: priority.effective(),
        forced_priority: priority.forced(),
        initial_job_id: Some(predecessor.initial_job_id()),
        requester: Some(*predecessor.requester()),
        passport: predecessor.passport().cloned(),
        payload: predecessor.payload().cloned(),
        call: predecessor.call().copied(),
    }))];
    let first_run_id = match command.dispatch {
        SuccessorDispatch::Push {
            first_run_id,
            max_run_duration,
        } => {
            successor.push(JobEvent::RunDispatched(RunDispatched {
                job_id: command.successor_job_id,
                run_id: first_run_id,
                runner_type: predecessor.runner_type().clone(),
                attempt_number: AttemptNumber::FIRST,
                origin: RunOrigin::ManualRetry,
                automatic_retry_schedule_id: None,
                automatic_retry_of_run_id: None,
                max_run_duration: Some(max_run_duration),
                claimed_by: None,
            }));
            Some(first_run_id)
        }
        SuccessorDispatch::AwaitClaim => None,
    };
    Ok(ManualRetryOutcome::Started(Box::new(ManualRetryPlan {
        predecessor: CommandResult::from_event(JobEvent::ManualRetryStarted(ManualRetryStarted {
            job_id: predecessor.id(),
            manual_retry_id: command.manual_retry_id,
            failed_resolution_id: command.failed_resolution_id,
            successor_job_id: command.successor_job_id,
            requested_by: command.requested_by,
            run_id: first_run_id,
        })),
        successor: CommandResult::new(successor),
    })))
}

fn guard_current_failure(
    predecessor: &Job,
    failed_resolution_id: ResolutionId,
) -> Result<(), JobsError> {
    let resolution = predecessor
        .resolution()
        .filter(|resolution| resolution.is_failure())
        .ok_or(JobsError::JobNotFailed {
            status: predecessor.status().as_db_str(),
        })?;
    if resolution.id() == failed_resolution_id {
        Ok(())
    } else {
        Err(JobsError::StaleFailedResolution {
            current_resolution_id: resolution.id().as_uuid(),
        })
    }
}

#[cfg(test)]
mod tests;
