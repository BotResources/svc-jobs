use br_core_events::UserId;

use crate::commands::{CommandResult, JobCommandResult};
use crate::domain::attempts::MaxAttempts;
use crate::domain::config::RunnerConfig;
use crate::domain::directory::caller_address::guard_caller_address_is_named;
use crate::domain::ids::{JobId, SourceEntityId};
use crate::domain::job::parenting::guard_parent_admits_work;
use crate::domain::job::{Job, JobCall};
use crate::domain::keys::{ProducerKey, RunnerTypeKey};
use crate::domain::ownership::JobOwner;
use crate::domain::passport::JobPassport;
use crate::domain::payload::JobPayload;
use crate::domain::policy::ServiceLimits;
use crate::domain::priority::{JobPriority, Priority, PriorityBasis, UserMaxPriority};
use crate::domain::references::{KnownUser, SourceReference};
use crate::domain::requester::Requester;
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
    pub requested_priority: Priority,
    pub initial_job_id: Option<JobId>,
    pub requester: Requester,
    pub passport: Option<JobPassport>,
    pub payload: Option<JobPayload>,
    pub call: Option<JobCall>,
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

    /// The KnownUser the new Job runs for, below `parent` when it has one —
    /// the one `create_job` stores, and whose maximum caps a root Job.
    ///
    /// A KnownUser the creation names must be the user of the Passport, and is
    /// kept with the name it states. Otherwise a user's Passport makes its user
    /// the KnownUser, known only by id: the creation states no name, so it
    /// changes none Jobs knows. A service's Passport runs for no user, and a Job
    /// left without a Passport (below a parent whose Passport was erased) runs
    /// for the user of its parent, again known only by id.
    pub fn user_the_work_runs_for(
        &self,
        parent: Option<&Job>,
    ) -> Result<Option<KnownUser>, JobsError> {
        let passport = resolve_passport(self, parent)?;
        known_user_for(self, parent, passport.as_ref())
    }
}

pub fn create_job(
    command: CreateJob,
    existing: Option<&Job>,
    parent: Option<&Job>,
    user_maximum: Option<&UserMaxPriority>,
    limits: &ServiceLimits,
) -> Result<CreateOutcome, JobsError> {
    if let Some(existing) = existing {
        if existing.id() != command.id {
            return Err(JobsError::CorruptState {
                reason_code: "existing_job_is_not_the_command_target",
            });
        }
        return if command.is_the_same_declaration_as(existing) {
            Ok(CreateOutcome::AlreadyQueued)
        } else {
            Err(JobsError::JobIdConflict {
                job_id: command.id.as_uuid(),
            })
        };
    }
    if let Some(requested) = command.max_attempts {
        requested.guard_under_ceiling(limits.max_attempts_ceiling())?;
    }
    let (owner, parent) = resolve_owner(&command, parent)?;
    let initial_job_id = resolve_initial_job(&command, parent)?;
    let passport = resolve_passport(&command, parent)?;
    let triggered_by = command.user_the_work_runs_for(parent)?;
    guard_maximum_is_of(user_maximum, triggered_by.as_ref())?;
    let call = resolve_call(&command)?;
    if let Some(payload) = &command.payload {
        payload.guard_inline_within(limits.inline_payload_max_bytes())?;
    }
    let priority = JobPriority::admitted(
        command.requested_priority,
        PriorityBasis::of(parent, user_maximum, limits),
    );
    let source = command.source();
    Ok(CreateOutcome::Queued(CommandResult::from_event(
        JobEvent::JobQueued(Box::new(JobQueued {
            job_id: command.id,
            runner_type: command.runner_type,
            producer: command.producer,
            config: command.config,
            owner,
            parent_job_id: command.parent_job_id,
            predecessor_job_id: None,
            triggered_by,
            source,
            max_attempts: command.max_attempts,
            requested_priority: priority.requested(),
            effective_priority: priority.effective(),
            forced_priority: priority.forced(),
            initial_job_id: initial_job_id.filter(|initial| *initial != command.id),
            requester: Some(command.requester),
            passport,
            payload: command.payload,
            call,
            restart_of_run_id: None,
            restart_of_job_id: None,
        })),
    )))
}

fn resolve_initial_job(
    command: &CreateJob,
    parent: Option<&Job>,
) -> Result<Option<JobId>, JobsError> {
    let expected = parent.map_or(command.id, Job::initial_job_id);
    match command.initial_job_id {
        Some(given) if given != expected => Err(JobsError::InitialJobMismatch),
        _ => Ok(Some(expected)),
    }
}

fn resolve_passport(
    command: &CreateJob,
    parent: Option<&Job>,
) -> Result<Option<JobPassport>, JobsError> {
    match (&command.passport, parent) {
        (Some(given), Some(parent)) => {
            given.guard_is_the_passport_of_parent(parent.passport())?;
            Ok(Some(given.clone()))
        }
        (None, Some(parent)) => Ok(parent.passport().cloned()),
        (Some(given), None) => Ok(Some(given.clone())),
        (None, None) => Err(JobsError::PassportRequired),
    }
}

fn known_user_for(
    command: &CreateJob,
    parent: Option<&Job>,
    passport: Option<&JobPassport>,
) -> Result<Option<KnownUser>, JobsError> {
    if let Some(given) = &command.triggered_by {
        if let Some(passport) = passport {
            passport.guard_matches_user(Some(given))?;
        }
        return Ok(Some(given.clone()));
    }
    match passport {
        Some(passport) => passport
            .user_id()
            .map(|user| KnownUser::id_only(UserId(user)))
            .transpose(),
        None => Ok(parent
            .and_then(Job::triggered_by)
            .map(KnownUser::without_name)),
    }
}

/// The maximum given to cap a Job is the one of the user it runs for, or none.
fn guard_maximum_is_of(
    user_maximum: Option<&UserMaxPriority>,
    runs_for: Option<&KnownUser>,
) -> Result<(), JobsError> {
    match (user_maximum, runs_for) {
        (None, _) => Ok(()),
        (Some(maximum), Some(user)) if maximum.user_id().as_uuid() == user.id().0 => Ok(()),
        (Some(_), _) => Err(JobsError::CorruptState {
            reason_code: "user_maximum_of_another_user",
        }),
    }
}

pub(super) fn resolve_call(command: &CreateJob) -> Result<Option<JobCall>, JobsError> {
    match (&command.requester, command.call) {
        (Requester::Run(_), None) => Err(JobsError::ModeRequired),
        (Requester::Run(_), Some(call)) => Ok(Some(JobCall {
            mode: call.mode,
            caller_address: guard_caller_address_is_named(call.mode, call.caller_address)?,
        })),
        (Requester::Service(_), _) => Ok(None),
    }
}

fn resolve_owner<'a>(
    command: &CreateJob,
    parent: Option<&'a Job>,
) -> Result<(JobOwner, Option<&'a Job>), JobsError> {
    let Some(parent_job_id) = command.parent_job_id else {
        if matches!(command.requester, Requester::Run(_)) {
            return Err(JobsError::ParentNotOfRun);
        }
        return Ok((JobOwner::Producer(command.producer.clone()), None));
    };
    if parent_job_id == command.id {
        return Err(JobsError::SelfReference {
            field: "parent_job_id",
        });
    }
    let parent = guard_parent_admits_work(parent_job_id, parent)?;
    Ok((
        JobOwner::Runner {
            parent_job_id,
            runner_type: parent.runner_type().clone(),
        },
        Some(parent),
    ))
}

mod repeat;

#[cfg(test)]
mod tests;
