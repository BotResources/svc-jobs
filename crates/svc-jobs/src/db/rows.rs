use bc_jobs::JobsError;
use bc_jobs::domain::attempts::{AttemptNumber, MaxAttempts};
use bc_jobs::domain::config::RunnerConfig;
use bc_jobs::domain::ids::{
    JobId, ManualRetryId, PlanDeclarationId, ResolutionId, RetryScheduleId, RunId, SourceEntityId,
};
use bc_jobs::domain::job::parts::{JobDeletion, ManualRetryRecord};
use bc_jobs::domain::job::resolution::{JobFailureCause, JobResolution, JobResolutionKind};
use bc_jobs::domain::keys::{DisplayName, InstanceKey, ReasonCode, RunnerTypeKey, StepLabel};
use bc_jobs::domain::references::KnownUser;
use bc_jobs::domain::run::failure::{RunFailureKind, RunFailureReport};
use bc_jobs::domain::run::parts::{
    RetrySchedule, RunCancellationRequest, RunStart, RunTerminal, RunnerInstanceReference,
};
use bc_jobs::domain::run::plan::{DeclarationNumber, RunPlan, RunPlanItem};
use bc_jobs::domain::run::status::RunTerminalKind;
use bc_jobs::domain::run::step::{Step, StepIndex};
use br_core_events::UserId;
use chrono::{DateTime, TimeDelta, Utc};
use sqlx::Row;
use sqlx::postgres::PgRow;
use uuid::Uuid;

pub fn known_user(id: Uuid, display_name: String) -> Result<KnownUser, JobsError> {
    KnownUser::new(UserId(id), DisplayName::new(display_name)?)
}

pub fn optional_known_user(
    id: Option<Uuid>,
    display_name: Option<String>,
) -> Result<Option<KnownUser>, JobsError> {
    match (id, display_name) {
        (Some(id), Some(name)) => known_user(id, name).map(Some),
        (None, _) => Ok(None),
        (Some(_), None) => Err(JobsError::CorruptState {
            reason_code: "known_user_without_display_name",
        }),
    }
}

pub fn attempt(row: &PgRow, column: &str) -> Result<AttemptNumber, JobsError> {
    AttemptNumber::new(u32::try_from(row.get::<i32, _>(column)).unwrap_or(0))
}

pub fn max_attempts(value: Option<i32>) -> Result<Option<MaxAttempts>, JobsError> {
    value
        .map(|declared| MaxAttempts::new(u32::try_from(declared).unwrap_or(0)))
        .transpose()
}

pub fn config(value: Option<serde_json::Value>) -> Result<Option<RunnerConfig>, JobsError> {
    value.map(RunnerConfig::new).transpose()
}

pub fn step_index(value: i32) -> StepIndex {
    StepIndex::new(u32::try_from(value).unwrap_or(0))
}

pub fn start(row: &PgRow) -> Result<(RunId, RunStart), JobsError> {
    let instance = RunnerInstanceReference::new(
        RunnerTypeKey::new(row.get::<String, _>("type_key"))?,
        InstanceKey::new(row.get::<String, _>("instance_key"))?,
    );
    Ok((
        RunId::new(row.get("run_id"))?,
        RunStart::new(instance, row.get("started_at")),
    ))
}

pub fn terminal(row: &PgRow) -> Result<(RunId, RunTerminal), JobsError> {
    let kind = RunTerminalKind::from_db_str(&row.get::<String, _>("kind"))?;
    let failure = match row.get::<Option<String>, _>("failure_kind") {
        None => None,
        Some(raw) => Some(RunFailureReport::new(
            RunFailureKind::from_db_str(&raw)?,
            ReasonCode::new(row.get::<Option<String>, _>("reason_code").ok_or(
                JobsError::CorruptState {
                    reason_code: "failed_run_without_reason_code",
                },
            )?)?,
            row.get::<Option<serde_json::Value>, _>("params")
                .unwrap_or_default(),
            row.get::<Option<serde_json::Value>, _>("diagnostic")
                .unwrap_or_default(),
            row.get::<Option<i64>, _>("retry_after_seconds")
                .map(TimeDelta::seconds),
        )?),
    };
    Ok((
        RunId::new(row.get("run_id"))?,
        RunTerminal::hydrate(kind, row.get("occurred_at"), failure)?,
    ))
}

pub fn retry_schedule(row: &PgRow) -> Result<(RunId, RetrySchedule), JobsError> {
    Ok((
        RunId::new(row.get("failed_run_id"))?,
        RetrySchedule::new(
            RetryScheduleId::new(row.get("id"))?,
            row.get::<DateTime<Utc>, _>("due_at"),
        ),
    ))
}

pub fn plan_item(row: &PgRow) -> Result<RunPlanItem, JobsError> {
    Ok(RunPlanItem::new(
        step_index(row.get::<i32, _>("step_index")),
        StepLabel::new(row.get::<String, _>("label"))?,
    ))
}

pub fn plan(
    declaration_id: Uuid,
    declaration_number: i32,
    declared_at: DateTime<Utc>,
    items: Vec<RunPlanItem>,
) -> Result<RunPlan, JobsError> {
    RunPlan::new(
        PlanDeclarationId::new(declaration_id)?,
        DeclarationNumber::new(u32::try_from(declaration_number).unwrap_or(0))?,
        declared_at,
        items,
    )
}

pub fn step(row: &PgRow) -> Result<(RunId, Step), JobsError> {
    Ok((
        RunId::new(row.get("run_id"))?,
        Step::new(
            step_index(row.get::<i32, _>("step_index")),
            StepLabel::new(row.get::<String, _>("label"))?,
            row.get("started_at"),
        ),
    ))
}

pub fn cancellation_request(row: &PgRow) -> Result<(RunId, RunCancellationRequest), JobsError> {
    let requested_by = optional_known_user(
        row.get::<Option<Uuid>, _>("requested_by_id"),
        row.get::<Option<String>, _>("display_name"),
    )?;
    let originating = row
        .get::<Option<Uuid>, _>("originating_job_id")
        .map(JobId::new)
        .transpose()?;
    Ok((
        RunId::new(row.get("run_id"))?,
        RunCancellationRequest::new(
            row.get("requested_at"),
            ReasonCode::new(row.get::<String, _>("reason_code"))?,
            requested_by,
            originating,
        ),
    ))
}

pub fn resolution(row: &PgRow) -> Result<(JobId, JobResolution), JobsError> {
    let cause = row
        .get::<Option<String>, _>("failure_cause")
        .map(|raw| JobFailureCause::from_db_str(&raw))
        .transpose()?;
    let caused_by = row
        .get::<Option<Uuid>, _>("caused_by_run_id")
        .map(RunId::new)
        .transpose()?;
    Ok((
        JobId::new(row.get("job_id"))?,
        JobResolution::hydrate(
            ResolutionId::new(row.get("id"))?,
            JobResolutionKind::from_db_str(&row.get::<String, _>("kind"))?,
            row.get("occurred_at"),
            cause,
            caused_by,
        )?,
    ))
}

pub fn deletion(row: &PgRow) -> Result<(JobId, JobDeletion), JobsError> {
    Ok((
        JobId::new(row.get("job_id"))?,
        JobDeletion::new(
            known_user(row.get("deleted_by_id"), row.get("display_name"))?,
            row.get("deleted_at"),
        ),
    ))
}

pub fn manual_retry(row: &PgRow) -> Result<(JobId, ManualRetryRecord), JobsError> {
    Ok((
        JobId::new(row.get("predecessor_job_id"))?,
        ManualRetryRecord::new(
            ManualRetryId::new(row.get("id"))?,
            ResolutionId::new(row.get("failed_resolution_id"))?,
            JobId::new(row.get("successor_job_id"))?,
            known_user(row.get("requested_by_id"), row.get("display_name"))?,
            row.get("requested_at"),
            row.get("successor_settled"),
        ),
    ))
}

pub fn source_entity(value: Option<Uuid>) -> Result<Option<SourceEntityId>, JobsError> {
    value.map(SourceEntityId::new).transpose()
}
