use std::sync::Arc;

use async_graphql::{Json, Object};
use bc_jobs::domain::job::Job;
use chrono::{DateTime, Utc};
use serde_json::Value;
use uuid::Uuid;

use super::enums::GqlJobStatus;
use super::job::{
    GqlJobDeletion, GqlJobResolution, GqlJobView, GqlKnownUser, GqlManualRetry, GqlSourceReference,
    manual_retry_of, resolution_of, source_of,
};
use super::origin::{self, GqlJobCall, GqlJobPassport, GqlJobPayload, GqlJobRequester};
use super::run::{GqlRun, GqlRunProgression, run_of};
use crate::edge::tree::JobTree;

pub struct GqlJobDetail {
    pub job: Arc<Job>,
    pub tree: Arc<JobTree>,
}

#[Object(name = "JobsJobDetail")]
impl GqlJobDetail {
    async fn id(&self) -> Uuid {
        self.job.id().as_uuid()
    }

    async fn runner_type(&self) -> String {
        self.job.runner_type().as_str().to_owned()
    }

    async fn producer(&self) -> String {
        self.job.producer().as_str().to_owned()
    }

    async fn config(&self) -> Option<Json<Value>> {
        self.job
            .config()
            .map(|config| Json(config.as_value().clone()))
    }

    async fn source(&self) -> Option<GqlSourceReference> {
        source_of(&self.job)
    }

    async fn parent_job_id(&self) -> Option<Uuid> {
        self.job.parent_job_id().map(|id| id.as_uuid())
    }

    async fn initial_job_id(&self) -> Uuid {
        self.job.initial_job_id().as_uuid()
    }

    async fn passport(&self) -> Option<GqlJobPassport> {
        origin::passport_of(&self.job)
    }

    async fn requester(&self) -> GqlJobRequester {
        origin::requester_of(&self.job)
    }

    async fn call(&self) -> Option<GqlJobCall> {
        origin::call_of(&self.job)
    }

    async fn payload(&self) -> Option<GqlJobPayload> {
        origin::payload_of(&self.job)
    }

    async fn triggered_by(&self) -> Option<GqlKnownUser> {
        self.job.triggered_by().map(GqlKnownUser::from)
    }

    async fn max_attempts(&self) -> Option<i32> {
        self.job
            .max_attempts()
            .map(|budget| i32::try_from(budget.get()).unwrap_or(i32::MAX))
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

    async fn manual_retry(&self) -> Option<GqlManualRetry> {
        manual_retry_of(&self.job)
    }

    async fn is_deleted(&self) -> bool {
        self.job.is_deleted()
    }

    async fn deletion(&self) -> Option<GqlJobDeletion> {
        self.job.deletion().map(|deletion| GqlJobDeletion {
            deleted_by: GqlKnownUser::from(deletion.deleted_by()),
            deleted_at: deletion.deleted_at(),
        })
    }

    async fn created_at(&self) -> DateTime<Utc> {
        self.job.created_at()
    }

    async fn content_erased_at(&self) -> Option<DateTime<Utc>> {
        self.job.content_erased_at()
    }

    async fn runs(&self) -> Vec<GqlRun> {
        self.job
            .runs()
            .iter()
            .map(|run| run_of(&self.job, run))
            .collect()
    }

    async fn children(&self) -> Vec<GqlJobView> {
        self.tree
            .children_of(self.job.id())
            .iter()
            .map(|child| GqlJobView {
                job: Arc::clone(child),
                parent: Some(Arc::clone(&self.job)),
                tree: Arc::clone(&self.tree),
            })
            .collect()
    }
}
