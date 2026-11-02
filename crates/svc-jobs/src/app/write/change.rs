use bc_jobs::domain::ids::{JobId, RunId};
use bc_jobs::domain::job::Job;
use bc_jobs::domain::keys::RunnerTypeKey;
use bc_jobs::domain::payload::CallMode;
use bc_jobs::domain::references::SourceReference;
use bc_jobs::event::job::JobEvent;
use br_core_events::EventMetadata;
use uuid::Uuid;

use super::premise::PriorityPremise;
use crate::app::integration;

pub struct JobChange {
    pub job_id: JobId,
    pub before: Option<Job>,
    pub events: Vec<JobEvent>,
    pub note: Option<String>,
    pub claimed_source: Option<SourceReference>,
    pub metadata: Option<EventMetadata>,
    pub answers: Vec<integration::Published>,
    pub priority_premise: Option<PriorityPremise>,
    pub descendants_premise: Option<Vec<Uuid>>,
    pub free_capacity_of: Option<RunnerTypeKey>,
    pub removes_directory: bool,
    pub restart: Option<RestartPremise>,
    pub caller_address: Option<CallerAddressPremise>,
}

#[derive(Debug, Clone, Copy)]
pub struct CallerAddressPremise {
    pub mode: CallMode,
    pub address: Option<Uuid>,
    pub caller: RunId,
}

#[derive(Debug, Clone, Copy)]
pub struct RestartPremise {
    pub origin_run: Uuid,
    pub tree: JobId,
}

impl JobChange {
    pub fn new(job_id: JobId, before: Option<Job>, events: Vec<JobEvent>) -> Self {
        Self {
            job_id,
            before,
            events,
            note: None,
            claimed_source: None,
            metadata: None,
            answers: Vec::new(),
            priority_premise: None,
            descendants_premise: None,
            free_capacity_of: None,
            removes_directory: false,
            restart: None,
            caller_address: None,
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.events.is_empty() && self.answers.is_empty() && !self.removes_directory
    }

    pub fn restarting(mut self, origin_run: Uuid, tree: JobId) -> Self {
        self.restart = Some(RestartPremise { origin_run, tree });
        self
    }

    pub fn holding_caller_address(mut self, premise: Option<CallerAddressPremise>) -> Self {
        self.caller_address = premise;
        self
    }

    pub fn removing_directory(mut self) -> Self {
        self.removes_directory = true;
        self
    }

    pub fn requiring_free_capacity(mut self, runner_type: RunnerTypeKey) -> Self {
        self.free_capacity_of = Some(runner_type);
        self
    }

    pub fn over_descendants(mut self, descendants: impl IntoIterator<Item = JobId>) -> Self {
        let mut ids: Vec<Uuid> = descendants.into_iter().map(|id| id.as_uuid()).collect();
        ids.sort_unstable();
        self.descendants_premise = Some(ids);
        self
    }

    pub fn on_priority_premise(mut self, premise: Option<PriorityPremise>) -> Self {
        self.priority_premise = premise;
        self
    }

    pub fn answering(mut self, answer: integration::Published) -> Self {
        self.answers.push(answer);
        self
    }

    pub fn with_metadata(mut self, metadata: EventMetadata) -> Self {
        self.metadata = Some(metadata);
        self
    }

    pub fn with_note(mut self, note: Option<String>) -> Self {
        self.note = note;
        self
    }

    pub fn claiming_source(mut self, source: Option<SourceReference>) -> Self {
        self.claimed_source = source;
        self
    }
}
