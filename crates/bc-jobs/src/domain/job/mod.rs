pub mod derived;
pub mod parts;
pub mod resolution;
pub mod status;

use chrono::{DateTime, Utc};

use crate::domain::attempts::MaxAttempts;
use crate::domain::config::RunnerConfig;
use crate::domain::ids::{JobId, SourceEntityId};
use crate::domain::job::parts::{JobDeletion, ManualRetryRecord};
use crate::domain::job::resolution::JobResolution;
use crate::domain::keys::{ProducerKey, RunnerTypeKey};
use crate::domain::ownership::JobOwner;
use crate::domain::references::KnownUser;
use crate::domain::run::Run;
use crate::error::JobsError;

#[derive(Debug, Clone)]
pub struct JobState {
    pub id: JobId,
    pub runner_type: RunnerTypeKey,
    pub producer: ProducerKey,
    pub config: Option<RunnerConfig>,
    pub parent_job_id: Option<JobId>,
    pub predecessor_job_id: Option<JobId>,
    pub triggered_by: Option<KnownUser>,
    pub source_entity_id: Option<SourceEntityId>,
    pub max_attempts: Option<MaxAttempts>,
    pub created_at: DateTime<Utc>,
    pub owner: JobOwner,
    pub runs: Vec<Run>,
    pub resolution: Option<JobResolution>,
    pub deletion: Option<JobDeletion>,
    pub manual_retry: Option<ManualRetryRecord>,
}

#[derive(Debug, Clone)]
pub struct Job {
    id: JobId,
    runner_type: RunnerTypeKey,
    producer: ProducerKey,
    config: Option<RunnerConfig>,
    parent_job_id: Option<JobId>,
    predecessor_job_id: Option<JobId>,
    triggered_by: Option<KnownUser>,
    source_entity_id: Option<SourceEntityId>,
    max_attempts: Option<MaxAttempts>,
    created_at: DateTime<Utc>,
    owner: JobOwner,
    runs: Vec<Run>,
    resolution: Option<JobResolution>,
    deletion: Option<JobDeletion>,
    manual_retry: Option<ManualRetryRecord>,
}

fn corrupt(reason_code: &'static str) -> JobsError {
    JobsError::CorruptState { reason_code }
}

impl Job {
    pub fn hydrate(state: JobState) -> Result<Self, JobsError> {
        if state.parent_job_id == Some(state.id) {
            return Err(JobsError::SelfReference {
                field: "parent_job_id",
            });
        }
        if state.predecessor_job_id == Some(state.id) {
            return Err(JobsError::SelfReference {
                field: "predecessor_job_id",
            });
        }
        if state.owner.parent_job_id() != state.parent_job_id {
            return Err(corrupt("owner_does_not_match_parent"));
        }
        if let JobOwner::Producer(owner) = &state.owner
            && owner != &state.producer
        {
            return Err(corrupt("owner_does_not_match_producer"));
        }
        let mut runs = state.runs;
        runs.sort_by_key(Run::attempt_number);
        for (position, run) in runs.iter().enumerate() {
            let expected = u32::try_from(position + 1).unwrap_or(u32::MAX);
            if run.attempt_number().get() != expected {
                return Err(corrupt("attempt_numbers_not_contiguous"));
            }
        }
        if runs.iter().filter(|run| !run.is_terminal()).count() > 1 {
            return Err(corrupt("several_non_terminal_runs"));
        }
        if let Some(resolution) = &state.resolution
            && let Some(caused_by) = resolution.caused_by_run_id()
            && !runs.iter().any(|run| run.id() == caused_by)
        {
            return Err(corrupt("resolution_names_a_foreign_run"));
        }
        if state.deletion.is_some() && state.resolution.is_none() {
            return Err(corrupt("deleted_job_without_resolution"));
        }
        if let Some(manual_retry) = &state.manual_retry {
            if manual_retry.successor_job_id() == state.id {
                return Err(JobsError::SelfReference {
                    field: "successor_job_id",
                });
            }
            let resolves_this_job = state
                .resolution
                .as_ref()
                .is_some_and(|resolution| resolution.id() == manual_retry.failed_resolution_id());
            if !resolves_this_job {
                return Err(corrupt("manual_retry_names_a_foreign_resolution"));
            }
        }
        Ok(Self {
            id: state.id,
            runner_type: state.runner_type,
            producer: state.producer,
            config: state.config,
            parent_job_id: state.parent_job_id,
            predecessor_job_id: state.predecessor_job_id,
            triggered_by: state.triggered_by,
            source_entity_id: state.source_entity_id,
            max_attempts: state.max_attempts,
            created_at: state.created_at,
            owner: state.owner,
            runs,
            resolution: state.resolution,
            deletion: state.deletion,
            manual_retry: state.manual_retry,
        })
    }

    pub fn id(&self) -> JobId {
        self.id
    }

    pub fn runner_type(&self) -> &RunnerTypeKey {
        &self.runner_type
    }

    pub fn producer(&self) -> &ProducerKey {
        &self.producer
    }

    pub fn config(&self) -> Option<&RunnerConfig> {
        self.config.as_ref()
    }

    pub fn parent_job_id(&self) -> Option<JobId> {
        self.parent_job_id
    }

    pub fn predecessor_job_id(&self) -> Option<JobId> {
        self.predecessor_job_id
    }

    pub fn triggered_by(&self) -> Option<&KnownUser> {
        self.triggered_by.as_ref()
    }

    pub fn source_entity_id(&self) -> Option<SourceEntityId> {
        self.source_entity_id
    }

    pub fn max_attempts(&self) -> Option<MaxAttempts> {
        self.max_attempts
    }

    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    pub fn owner(&self) -> &JobOwner {
        &self.owner
    }

    pub fn runs(&self) -> &[Run] {
        &self.runs
    }

    pub fn resolution(&self) -> Option<&JobResolution> {
        self.resolution.as_ref()
    }

    pub fn deletion(&self) -> Option<&JobDeletion> {
        self.deletion.as_ref()
    }

    pub fn manual_retry(&self) -> Option<&ManualRetryRecord> {
        self.manual_retry.as_ref()
    }
}
