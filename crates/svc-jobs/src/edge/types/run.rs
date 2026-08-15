use async_graphql::{Json, SimpleObject};
use bc_jobs::domain::job::Job;
use bc_jobs::domain::run::Run;
use bc_jobs::domain::run::parts::RunnerInstanceReference;
use bc_jobs::domain::run::plan::RunPlan;
use bc_jobs::domain::run::progression::RunProgression;
use bc_jobs::domain::run::step::Step;
use chrono::{DateTime, Utc};
use serde_json::Value;
use uuid::Uuid;

use super::enums::{GqlRunFailureKind, GqlRunOrigin, GqlRunStatus};

#[derive(SimpleObject)]
#[graphql(name = "JobsRunnerInstanceReference")]
pub struct GqlInstanceReference {
    pub runner_type: String,
    pub instance_key: String,
}

impl From<&RunnerInstanceReference> for GqlInstanceReference {
    fn from(instance: &RunnerInstanceReference) -> Self {
        Self {
            runner_type: instance.runner_type().as_str().to_owned(),
            instance_key: instance.instance_key().as_str().to_owned(),
        }
    }
}

#[derive(SimpleObject)]
#[graphql(name = "JobsRunPlanItem")]
pub struct GqlRunPlanItem {
    pub index: i32,
    pub label: String,
}

#[derive(SimpleObject)]
#[graphql(name = "JobsRunPlan")]
pub struct GqlRunPlan {
    pub declaration_id: Uuid,
    pub declaration_number: i32,
    pub declared_at: DateTime<Utc>,
    pub items: Vec<GqlRunPlanItem>,
}

impl From<&RunPlan> for GqlRunPlan {
    fn from(plan: &RunPlan) -> Self {
        Self {
            declaration_id: plan.declaration_id().as_uuid(),
            declaration_number: i32::try_from(plan.declaration_number().get()).unwrap_or(i32::MAX),
            declared_at: plan.declared_at(),
            items: plan
                .items()
                .iter()
                .map(|item| GqlRunPlanItem {
                    index: i32::try_from(item.index().get()).unwrap_or(i32::MAX),
                    label: item.label().as_str().to_owned(),
                })
                .collect(),
        }
    }
}

#[derive(SimpleObject)]
#[graphql(name = "JobsStep")]
pub struct GqlStep {
    pub index: i32,
    pub label: String,
    pub started_at: DateTime<Utc>,
}

impl From<&Step> for GqlStep {
    fn from(step: &Step) -> Self {
        Self {
            index: i32::try_from(step.index().get()).unwrap_or(i32::MAX),
            label: step.label().as_str().to_owned(),
            started_at: step.started_at(),
        }
    }
}

#[derive(SimpleObject)]
#[graphql(name = "JobsRunProgression")]
pub struct GqlRunProgression {
    pub plan: Option<GqlRunPlan>,
    pub current_step: Option<GqlStep>,
}

impl From<RunProgression<'_>> for GqlRunProgression {
    fn from(progression: RunProgression<'_>) -> Self {
        Self {
            plan: progression.plan().map(GqlRunPlan::from),
            current_step: progression.current_step().map(GqlStep::from),
        }
    }
}

#[derive(SimpleObject)]
#[graphql(name = "JobsRunFailureReport")]
pub struct GqlRunFailureReport {
    pub kind: GqlRunFailureKind,
    pub reason_code: String,
    pub params: Json<Value>,
    pub diagnostic: Json<Value>,
    pub retry_after_seconds: Option<i32>,
}

#[derive(SimpleObject)]
#[graphql(name = "JobsRun")]
pub struct GqlRun {
    pub id: Uuid,
    pub job_id: Uuid,
    pub attempt_number: i32,
    pub origin: GqlRunOrigin,
    pub automatic_retry_of_run_id: Option<Uuid>,
    pub instance: Option<GqlInstanceReference>,
    pub status: GqlRunStatus,
    pub declared_plan: Option<GqlRunPlan>,
    pub progression: Option<GqlRunProgression>,
    pub steps: Vec<GqlStep>,
    pub failure_report: Option<GqlRunFailureReport>,
    pub retry_due_at: Option<DateTime<Utc>>,
    pub cancellation_requested_at: Option<DateTime<Utc>>,
    pub dispatched_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
}

pub fn run_of(job: &Job, run: &Run) -> GqlRun {
    GqlRun {
        id: run.id().as_uuid(),
        job_id: job.id().as_uuid(),
        attempt_number: i32::try_from(run.attempt_number().get()).unwrap_or(i32::MAX),
        origin: run.origin(job.has_predecessor()).into(),
        automatic_retry_of_run_id: job.automatic_retry_of_run_id(run).map(|id| id.as_uuid()),
        instance: run.start().map(|start| start.instance().into()),
        status: run.status().into(),
        declared_plan: run.plan().map(GqlRunPlan::from),
        progression: run.progression().map(GqlRunProgression::from),
        steps: run.steps().iter().map(GqlStep::from).collect(),
        failure_report: run
            .terminal()
            .and_then(|terminal| terminal.failure())
            .map(|report| GqlRunFailureReport {
                kind: report.kind().into(),
                reason_code: report.reason_code().as_str().to_owned(),
                params: Json(report.params().clone()),
                diagnostic: Json(report.diagnostic().clone()),
                retry_after_seconds: report
                    .retry_after()
                    .map(|hint| i32::try_from(hint.num_seconds()).unwrap_or(i32::MAX)),
            }),
        retry_due_at: run.retry_schedule().map(|schedule| schedule.due_at()),
        cancellation_requested_at: run
            .cancellation_request()
            .map(|request| request.requested_at()),
        dispatched_at: run.dispatched_at(),
        started_at: run.start().map(|start| start.started_at()),
        finished_at: run.terminal().map(|terminal| terminal.occurred_at()),
    }
}
