//! A Job as the user it runs for sees it: no Passport, no payload, no
//! configuration, no requester.

use std::sync::Arc;

use async_graphql::{Context, Object, Result};
use bc_jobs::domain::job::Job;
use br_util_graphql::Affordance;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use super::enums::GqlJobStatus;
use super::job::{GqlJobResolution, resolution_of};
use super::run::{GqlRun, GqlRunProgression, run_of};
use crate::edge::project;
use crate::edge::state::principal;

pub struct GqlMyJobView {
    pub job: Arc<Job>,
    pub parent: Option<Arc<Job>>,
}

#[Object(name = "JobsMyJobView")]
impl GqlMyJobView {
    async fn job(&self) -> GqlMyJob {
        GqlMyJob {
            job: Arc::clone(&self.job),
        }
    }

    /// `cancel` as the caller may use it; the administrators' gestures show as
    /// not allowed.
    async fn affordances(&self, ctx: &Context<'_>) -> Result<Vec<Affordance>> {
        project::affordances_of(&self.job, self.parent.as_deref(), &principal(ctx)?)
    }
}

pub struct GqlMyJob {
    pub job: Arc<Job>,
}

#[Object(name = "JobsMyJob")]
impl GqlMyJob {
    async fn id(&self) -> Uuid {
        self.job.id().as_uuid()
    }

    async fn runner_type(&self) -> String {
        self.job.runner_type().as_str().to_owned()
    }

    async fn parent_job_id(&self) -> Option<Uuid> {
        self.job.parent_job_id().map(|id| id.as_uuid())
    }

    async fn initial_job_id(&self) -> Uuid {
        self.job.initial_job_id().as_uuid()
    }

    async fn status(&self) -> GqlJobStatus {
        self.job.status().into()
    }

    async fn requested_priority(&self) -> i32 {
        i32::from(self.job.priority().requested().get())
    }

    async fn effective_priority(&self) -> i32 {
        i32::from(self.job.priority().effective().get())
    }

    async fn forced_priority(&self) -> Option<i32> {
        self.job
            .priority()
            .forced()
            .map(|level| i32::from(level.get()))
    }

    async fn attempt_count(&self) -> i32 {
        i32::try_from(self.job.attempts_reached()).unwrap_or(i32::MAX)
    }

    async fn active_run_id(&self) -> Option<Uuid> {
        self.job.active_run().map(|run| run.id().as_uuid())
    }

    async fn progression(&self) -> Option<GqlRunProgression> {
        self.job.progression().map(GqlRunProgression::from)
    }

    async fn next_attempt_at(&self) -> Option<DateTime<Utc>> {
        self.job.next_attempt_at()
    }

    async fn resolution(&self) -> Option<GqlJobResolution> {
        resolution_of(&self.job)
    }

    async fn created_at(&self) -> DateTime<Utc> {
        self.job.created_at()
    }

    async fn content_erased_at(&self) -> Option<DateTime<Utc>> {
        self.job.content_erased_at()
    }

    /// Every Run, with its failure report when it failed.
    async fn runs(&self) -> Vec<GqlRun> {
        self.job
            .runs()
            .iter()
            .map(|run| run_of(&self.job, run))
            .collect()
    }
}
