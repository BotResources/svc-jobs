use chrono::{DateTime, Utc};

use super::parts::{JobDeletion, ManualRetryRecord};
use super::resolution::JobResolution;
use super::{Job, JobCall};
use crate::domain::attempts::MaxAttempts;
use crate::domain::config::RunnerConfig;
use crate::domain::ids::{JobId, RunId, SourceEntityId};
use crate::domain::keys::{ProducerKey, RunnerTypeKey};
use crate::domain::ownership::JobOwner;
use crate::domain::passport::JobPassport;
use crate::domain::payload::JobPayload;
use crate::domain::priority::JobPriority;
use crate::domain::references::KnownUser;
use crate::domain::requester::Requester;
use crate::domain::run::Run;

impl Job {
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

    pub fn priority(&self) -> &JobPriority {
        &self.priority
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

    pub fn initial_job_id(&self) -> JobId {
        self.initial_job_id
    }

    pub fn wire_parent_job_id(&self) -> JobId {
        self.parent_job_id.unwrap_or(self.id)
    }

    pub fn requester(&self) -> &Requester {
        &self.requester
    }

    pub fn passport(&self) -> Option<&JobPassport> {
        self.passport.as_ref()
    }

    pub fn payload(&self) -> Option<&JobPayload> {
        self.payload.as_ref()
    }

    pub fn call(&self) -> Option<&JobCall> {
        self.call.as_ref()
    }

    pub fn restart_of_run_id(&self) -> Option<RunId> {
        self.restart_of_run_id
    }

    pub fn content_erased_at(&self) -> Option<DateTime<Utc>> {
        self.content_erased_at
    }

    pub fn is_restart(&self) -> bool {
        self.restart_of_run_id.is_some()
    }
}
