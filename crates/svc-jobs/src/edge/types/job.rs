use std::sync::Arc;

use async_graphql::{Context, Object, Result, SimpleObject};
use bc_jobs::domain::job::Job;
use bc_jobs::domain::references::KnownUser;
use br_util_graphql::Affordance;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use super::enums::{GqlJobFailureCause, GqlJobResolutionKind, GqlJobStatus};
use super::job_detail::GqlJobDetail;
use super::run::GqlRunProgression;
use crate::edge::project;
use crate::edge::state::principal;
use crate::edge::tree::JobTree;

#[derive(SimpleObject, Clone)]
#[graphql(name = "JobsKnownUser")]
pub struct GqlKnownUser {
    pub id: Uuid,
    pub display_name: String,
}

impl From<&KnownUser> for GqlKnownUser {
    fn from(user: &KnownUser) -> Self {
        Self {
            id: user.id().0,
            display_name: user.shown_name(),
        }
    }
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "JobsSourceReference")]
pub struct GqlSourceReference {
    pub bc: String,
    pub entity_id: Uuid,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "JobsJobResolution")]
pub struct GqlJobResolution {
    pub id: Uuid,
    pub kind: GqlJobResolutionKind,
    pub occurred_at: DateTime<Utc>,
    pub failure_cause: Option<GqlJobFailureCause>,
    pub caused_by_run_id: Option<Uuid>,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "JobsJobDeletion")]
pub struct GqlJobDeletion {
    pub deleted_by: GqlKnownUser,
    pub deleted_at: DateTime<Utc>,
}

#[derive(SimpleObject, Clone)]
#[graphql(name = "JobsManualRetry")]
pub struct GqlManualRetry {
    pub id: Uuid,
    pub failed_resolution_id: Uuid,
    pub predecessor_job_id: Uuid,
    pub successor_job_id: Uuid,
    pub requested_by: GqlKnownUser,
    pub requested_at: DateTime<Utc>,
}

#[derive(SimpleObject)]
#[graphql(name = "JobsJobSummary")]
pub struct GqlJobSummary {
    pub id: Uuid,
    pub runner_type: String,
    pub producer: String,
    pub source: Option<GqlSourceReference>,
    pub parent_job_id: Option<Uuid>,
    pub predecessor_job_id: Option<Uuid>,
    pub successor_job_id: Option<Uuid>,
    pub triggered_by: Option<GqlKnownUser>,
    pub status: GqlJobStatus,
    pub requested_priority: i32,
    pub effective_priority: i32,
    pub forced_priority: Option<i32>,
    pub attempt_count: i32,
    pub active_run_id: Option<Uuid>,
    pub progression: Option<GqlRunProgression>,
    pub next_attempt_at: Option<DateTime<Utc>>,
    pub failure_cause: Option<GqlJobFailureCause>,
    pub is_deleted: bool,
    pub created_at: DateTime<Utc>,
}

pub struct GqlJobView {
    pub job: Arc<Job>,
    pub parent: Option<Arc<Job>>,
    pub tree: Arc<JobTree>,
}

#[Object(name = "JobsJobView")]
impl GqlJobView {
    async fn job(&self) -> GqlJobDetail {
        GqlJobDetail {
            job: Arc::clone(&self.job),
            tree: Arc::clone(&self.tree),
        }
    }

    async fn affordances(&self, ctx: &Context<'_>) -> Result<Vec<Affordance>> {
        project::affordances_of(&self.job, self.parent.as_deref(), &principal(ctx)?)
    }
}

#[derive(Clone)]
pub struct GqlJobSummaryView {
    pub job: Arc<Job>,
    pub parent: Option<Arc<Job>>,
}

#[Object(name = "JobsJobSummaryView")]
impl GqlJobSummaryView {
    async fn job(&self) -> GqlJobSummary {
        summary_of(&self.job)
    }

    async fn affordances(&self, ctx: &Context<'_>) -> Result<Vec<Affordance>> {
        project::affordances_of(&self.job, self.parent.as_deref(), &principal(ctx)?)
    }
}

pub fn source_of(job: &Job) -> Option<GqlSourceReference> {
    job.source().map(|source| GqlSourceReference {
        bc: source.bc().as_str().to_owned(),
        entity_id: source.entity_id().as_uuid(),
    })
}

pub fn resolution_of(job: &Job) -> Option<GqlJobResolution> {
    job.resolution().map(|resolution| GqlJobResolution {
        id: resolution.id().as_uuid(),
        kind: resolution.kind().into(),
        occurred_at: resolution.occurred_at(),
        failure_cause: resolution.failure_cause().map(GqlJobFailureCause::from),
        caused_by_run_id: resolution.caused_by_run_id().map(|id| id.as_uuid()),
    })
}

pub fn manual_retry_of(job: &Job) -> Option<GqlManualRetry> {
    job.manual_retry().map(|record| GqlManualRetry {
        id: record.id().as_uuid(),
        failed_resolution_id: record.failed_resolution_id().as_uuid(),
        predecessor_job_id: job.id().as_uuid(),
        successor_job_id: record.successor_job_id().as_uuid(),
        requested_by: GqlKnownUser::from(record.requested_by()),
        requested_at: record.requested_at(),
    })
}

pub fn summary_of(job: &Job) -> GqlJobSummary {
    GqlJobSummary {
        id: job.id().as_uuid(),
        runner_type: job.runner_type().as_str().to_owned(),
        producer: job.producer().as_str().to_owned(),
        source: source_of(job),
        parent_job_id: job.parent_job_id().map(|id| id.as_uuid()),
        predecessor_job_id: job.predecessor_job_id().map(|id| id.as_uuid()),
        successor_job_id: job
            .manual_retry()
            .map(|record| record.successor_job_id().as_uuid()),
        triggered_by: job.triggered_by().map(GqlKnownUser::from),
        status: job.status().into(),
        requested_priority: i32::from(job.priority().requested().get()),
        effective_priority: i32::from(job.priority().effective().get()),
        forced_priority: job.priority().forced().map(|level| i32::from(level.get())),
        attempt_count: i32::try_from(job.attempts_reached()).unwrap_or(i32::MAX),
        active_run_id: job.active_run().map(|run| run.id().as_uuid()),
        progression: job.progression().map(GqlRunProgression::from),
        next_attempt_at: job.next_attempt_at(),
        failure_cause: job
            .resolution()
            .and_then(|resolution| resolution.failure_cause())
            .map(GqlJobFailureCause::from),
        is_deleted: job.is_deleted(),
        created_at: job.created_at(),
    }
}
