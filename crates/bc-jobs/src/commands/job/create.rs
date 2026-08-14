use crate::commands::{CommandResult, JobCommandResult};
use crate::domain::attempts::MaxAttempts;
use crate::domain::config::RunnerConfig;
use crate::domain::ids::{JobId, SourceEntityId};
use crate::domain::job::Job;
use crate::domain::keys::{ProducerKey, RunnerTypeKey};
use crate::domain::ownership::JobOwner;
use crate::domain::policy::ServiceLimits;
use crate::domain::references::{KnownUser, SourceReference};
use crate::error::JobsError;
use crate::event::job::JobEvent;
use crate::event::job_facts::JobQueued;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateJob {
    pub id: JobId,
    pub runner_type: RunnerTypeKey,
    pub producer: ProducerKey,
    pub config: Option<RunnerConfig>,
    pub parent_job_id: Option<JobId>,
    pub triggered_by: Option<KnownUser>,
    pub source_entity_id: Option<SourceEntityId>,
    pub max_attempts: Option<MaxAttempts>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CreateOutcome {
    Queued(JobCommandResult),
    AlreadyQueued,
}

impl CreateJob {
    pub fn source(&self) -> Option<SourceReference> {
        self.source_entity_id
            .map(|entity_id| SourceReference::new(self.producer.clone(), entity_id))
    }

    fn is_the_same_declaration_as(&self, existing: &Job) -> bool {
        existing.runner_type() == &self.runner_type
            && existing.producer() == &self.producer
            && existing.config() == self.config.as_ref()
            && existing.parent_job_id() == self.parent_job_id
            && existing.triggered_by() == self.triggered_by.as_ref()
            && existing.source_entity_id() == self.source_entity_id
            && existing.max_attempts() == self.max_attempts
    }
}

pub fn create_job(
    command: CreateJob,
    existing: Option<&Job>,
    active_for_source: Option<&Job>,
    parent: Option<&Job>,
    limits: &ServiceLimits,
) -> Result<CreateOutcome, JobsError> {
    if let Some(existing) = existing {
        return if command.is_the_same_declaration_as(existing) {
            Ok(CreateOutcome::AlreadyQueued)
        } else {
            Err(JobsError::JobIdConflict {
                job_id: command.id.as_uuid(),
            })
        };
    }
    if let Some(requested) = command.max_attempts
        && requested.get() > limits.max_attempts_ceiling
    {
        return Err(JobsError::MaxAttemptsAboveCeiling {
            requested: requested.get(),
            ceiling: limits.max_attempts_ceiling,
        });
    }
    if let Some(active) = active_for_source
        && !active.is_terminal()
    {
        return Err(JobsError::SourceAlreadyActive {
            active_job_id: active.id().as_uuid(),
        });
    }
    resolve_owner(&command, parent)?;
    let source = command.source();
    Ok(CreateOutcome::Queued(CommandResult::from_event(
        JobEvent::JobQueued(JobQueued {
            job_id: command.id,
            runner_type: command.runner_type,
            producer: command.producer,
            parent_job_id: command.parent_job_id,
            predecessor_job_id: None,
            triggered_by: command.triggered_by,
            source,
            max_attempts: command.max_attempts,
        }),
    )))
}

fn resolve_owner(command: &CreateJob, parent: Option<&Job>) -> Result<JobOwner, JobsError> {
    let Some(parent_job_id) = command.parent_job_id else {
        return Ok(JobOwner::Producer(command.producer.clone()));
    };
    if parent_job_id == command.id {
        return Err(JobsError::SelfReference {
            field: "parent_job_id",
        });
    }
    let parent = parent
        .filter(|candidate| candidate.id() == parent_job_id)
        .ok_or(JobsError::ParentJobUnknown {
            parent_job_id: parent_job_id.as_uuid(),
        })?;
    if parent.is_deleted() {
        return Err(JobsError::ParentJobDeleted {
            parent_job_id: parent_job_id.as_uuid(),
        });
    }
    if parent.is_terminal() {
        return Err(JobsError::ParentJobTerminal {
            parent_job_id: parent_job_id.as_uuid(),
        });
    }
    Ok(JobOwner::Runner {
        parent_job_id,
        runner_type: parent.runner_type().clone(),
    })
}

#[cfg(test)]
mod tests;
