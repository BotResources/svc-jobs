use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::domain::attempts::{AttemptNumber, MaxAttempts};
use crate::domain::config::RunnerConfig;
use crate::domain::ids::{
    JobId, ManualRetryId, PlanDeclarationId, ResolutionId, RetryScheduleId, RunId,
};
use crate::domain::job::resolution::JobFailureCause;
use crate::domain::keys::{InstanceKey, ProducerKey, ReasonCode, RunnerTypeKey, StepLabel};
use crate::domain::ownership::JobOwner;
use crate::domain::references::{KnownUser, SourceReference};
use crate::domain::run::failure::RunFailureReport;
use crate::domain::run::origin::RunOrigin;
use crate::domain::run::plan::{DeclarationNumber, RunPlanItem};
use crate::domain::run::step::StepIndex;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobQueued {
    pub job_id: JobId,
    pub runner_type: RunnerTypeKey,
    pub producer: ProducerKey,
    pub config: Option<RunnerConfig>,
    pub owner: JobOwner,
    pub parent_job_id: Option<JobId>,
    pub predecessor_job_id: Option<JobId>,
    pub triggered_by: Option<KnownUser>,
    pub source: Option<SourceReference>,
    pub max_attempts: Option<MaxAttempts>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunDispatched {
    pub job_id: JobId,
    pub run_id: RunId,
    pub runner_type: RunnerTypeKey,
    pub attempt_number: AttemptNumber,
    pub origin: RunOrigin,
    pub automatic_retry_schedule_id: Option<RetryScheduleId>,
    pub automatic_retry_of_run_id: Option<RunId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunStarted {
    pub job_id: JobId,
    pub run_id: RunId,
    pub runner_type: RunnerTypeKey,
    pub instance_key: InstanceKey,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunPlanDeclared {
    pub job_id: JobId,
    pub run_id: RunId,
    pub declaration_id: PlanDeclarationId,
    pub declaration_number: DeclarationNumber,
    pub items: Vec<RunPlanItem>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunStepStarted {
    pub job_id: JobId,
    pub run_id: RunId,
    pub step_index: StepIndex,
    pub label: StepLabel,
    pub closed_step_index: Option<StepIndex>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunCompleted {
    pub job_id: JobId,
    pub run_id: RunId,
    pub attempt_number: AttemptNumber,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunFailed {
    pub job_id: JobId,
    pub run_id: RunId,
    pub attempt_number: AttemptNumber,
    pub report: RunFailureReport,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunCancelled {
    pub job_id: JobId,
    pub run_id: RunId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunCancellationRequested {
    pub job_id: JobId,
    pub run_id: RunId,
    pub reason_code: ReasonCode,
    pub requested_by: Option<KnownUser>,
    pub originating_job_id: Option<JobId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryScheduled {
    pub job_id: JobId,
    pub schedule_id: RetryScheduleId,
    pub failed_run_id: RunId,
    pub next_attempt_number: AttemptNumber,
    pub due_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobCompleted {
    pub job_id: JobId,
    pub resolution_id: ResolutionId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobFailed {
    pub job_id: JobId,
    pub resolution_id: ResolutionId,
    pub failure_cause: JobFailureCause,
    pub caused_by_run_id: Option<RunId>,
    pub report: Option<RunFailureReport>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobCancelled {
    pub job_id: JobId,
    pub resolution_id: ResolutionId,
    pub originating_job_id: Option<JobId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManualRetryStarted {
    pub job_id: JobId,
    pub manual_retry_id: ManualRetryId,
    pub failed_resolution_id: ResolutionId,
    pub successor_job_id: JobId,
    pub requested_by: KnownUser,
    pub run_id: RunId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobDeleted {
    pub job_id: JobId,
    pub deleted_by: KnownUser,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobAffordancesChanged {
    pub job_id: JobId,
    pub caused_by_job_id: Option<JobId>,
}
