use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::domain::attempts::{AttemptNumber, MaxAttempts};
use crate::domain::config::RunnerConfig;
use crate::domain::fleet::run_duration::MaxRunDuration;
use crate::domain::ids::{
    ClaimId, JobId, KnownUserId, ManualRetryId, PlanDeclarationId, ResolutionId, RetryScheduleId,
    RunId,
};
use crate::domain::job::JobCall;
use crate::domain::job::resolution::JobFailureCause;
use crate::domain::keys::{InstanceKey, ProducerKey, ReasonCode, RunnerTypeKey, StepLabel};
use crate::domain::ownership::JobOwner;
use crate::domain::passport::JobPassport;
use crate::domain::payload::JobPayload;
use crate::domain::priority::Priority;
use crate::domain::references::{KnownUser, SourceReference};
use crate::domain::requester::Requester;
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
    #[serde(default = "default_priority")]
    pub requested_priority: Priority,
    #[serde(default = "default_priority")]
    pub effective_priority: Priority,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub forced_priority: Option<Priority>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initial_job_id: Option<JobId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requester: Option<Requester>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub passport: Option<JobPassport>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<JobPayload>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub call: Option<JobCall>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restart_of_run_id: Option<RunId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restart_of_job_id: Option<JobId>,
}

fn default_priority() -> Priority {
    Priority::DEFAULT
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
    #[serde(default)]
    pub max_run_duration: Option<MaxRunDuration>,
    #[serde(default)]
    pub claimed_by: Option<RunClaim>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunClaim {
    pub instance_key: InstanceKey,
    pub claim_id: ClaimId,
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
    pub started_at: DateTime<Utc>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub withdrawn_for: Option<ReasonCode>,
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
    pub run_id: Option<RunId>,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobPriorityForced {
    pub job_id: JobId,
    pub priority: Priority,
    pub forced_by: KnownUser,
    pub originating_job_id: Option<JobId>,
    #[serde(default)]
    pub requested_priority: Option<Priority>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobPriorityRecalculated {
    pub job_id: JobId,
    pub effective_priority: Priority,
    pub previous_priority: Priority,
    #[serde(default)]
    pub user_id: Option<KnownUserId>,
    #[serde(default)]
    pub requested_priority: Option<Priority>,
}
