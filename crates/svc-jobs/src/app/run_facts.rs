use bc_jobs::JobsError;
use bc_jobs::commands::job::run_outcome::{RunCompletedFact, RunFailureFact};
use bc_jobs::commands::job::run_progress::{RunPlanFact, RunStartedFact, RunStepFact};
use bc_jobs::domain::ids::{PlanDeclarationId, ResolutionId, RetryScheduleId, RunId};
use bc_jobs::domain::keys::{InstanceKey, ReasonCode, RunnerTypeKey, StepLabel};
use bc_jobs::domain::run::failure::{RunFailureKind, RunFailureReport};
use bc_jobs::domain::run::parts::RunnerInstanceReference;
use bc_jobs::domain::run::plan::RunPlanItem;
use bc_jobs::domain::run::step::StepIndex;
use bc_jobs::{CommandResult, JobCommandResult};
use chrono::TimeDelta;
use contract_jobs::runner as wire;

use super::write::JobChange;
use super::{Jobs, service_metadata};
use crate::error::ServiceError;

pub async fn started(
    jobs: &Jobs,
    runner_type: &RunnerTypeKey,
    fact: &wire::RunStarted,
) -> Result<(), ServiceError> {
    let instance =
        RunnerInstanceReference::new(runner_type.clone(), InstanceKey::new(&fact.instance_key)?);
    apply(jobs, fact.run_id, |job| {
        job.record_run_started(RunStartedFact {
            run_id: RunId::new(fact.run_id)?,
            instance: instance.clone(),
        })
    })
    .await
}

pub async fn plan_declared(jobs: &Jobs, fact: &wire::PlanDeclared) -> Result<(), ServiceError> {
    let declaration_id = declaration_identity(jobs, fact.declaration_id)?;
    let mut items = Vec::with_capacity(fact.steps.len());
    for (index, label) in fact.steps.iter().enumerate() {
        items.push(RunPlanItem::new(
            StepIndex::new(u32::try_from(index).unwrap_or(0)),
            StepLabel::new(label)?,
        ));
    }
    let declared_at = jobs.clock.now();
    apply(jobs, fact.run_id, |job| {
        job.declare_run_plan(RunPlanFact {
            run_id: RunId::new(fact.run_id)?,
            declaration_id,
            items: items.clone(),
            declared_at,
        })
    })
    .await
}

pub async fn step_started(jobs: &Jobs, fact: &wire::StepStarted) -> Result<(), ServiceError> {
    let label = StepLabel::new(&fact.label)?;
    apply(jobs, fact.run_id, |job| {
        job.record_step_started(RunStepFact {
            run_id: RunId::new(fact.run_id)?,
            step_index: StepIndex::new(fact.index),
            label: label.clone(),
            started_at: fact.started_at,
        })
    })
    .await
}

pub async fn completed(jobs: &Jobs, fact: &wire::RunCompleted) -> Result<(), ServiceError> {
    let outcome = apply(jobs, fact.run_id, |job| {
        job.record_run_completed(RunCompletedFact {
            run_id: RunId::new(fact.run_id)?,
        })
    })
    .await;
    withdraw_stop(jobs, fact.run_id).await;
    outcome
}

pub async fn failed(jobs: &Jobs, fact: &wire::RunFailed) -> Result<(), ServiceError> {
    let report = RunFailureReport::new(
        RunFailureKind::from_declared(fact.report.kind.map(declared_kind)),
        ReasonCode::new(&fact.report.reason_code)?,
        object_or_empty(&fact.report.params),
        object_or_empty(&fact.report.diagnostic),
        fact.retry_after_seconds.map(TimeDelta::seconds),
    )?;
    let at = jobs.clock.now();
    let jitter = jobs.jitter.draw();
    let retry_schedule_id = RetryScheduleId::new(jobs.ids.next())?;
    let resolution_id = ResolutionId::new(jobs.ids.next())?;
    let outcome = apply(jobs, fact.run_id, |job| {
        job.record_run_failed(
            RunFailureFact {
                run_id: RunId::new(fact.run_id)?,
                report: report.clone(),
                retry_schedule_id,
                resolution_id,
                jitter,
                at,
            },
            &jobs.retry,
            &jobs.limits,
        )
    })
    .await;
    withdraw_stop(jobs, fact.run_id).await;
    outcome
}

fn declaration_identity(
    jobs: &Jobs,
    declared: Option<uuid::Uuid>,
) -> Result<PlanDeclarationId, ServiceError> {
    if let Some(declared) = declared {
        match PlanDeclarationId::new(declared) {
            Ok(identity) => return Ok(identity),
            Err(refusal) => tracing::warn!(
                declaration_id = %declared,
                error = %refusal,
                "a runner declared a plan under an identity a redelivery cannot be absorbed by; \
                 the declaration is recorded under a minted one"
            ),
        }
    }
    Ok(PlanDeclarationId::new(jobs.ids.next())?)
}

/// The published vocabulary carried over to the domain's own. Total on purpose:
/// a kind added to the contract stops this file compiling instead of being
/// rounded to something the domain already knows. The absent case is not
/// decided here — the domain owns that rule.
fn declared_kind(declared: wire::FailureKind) -> RunFailureKind {
    match declared {
        wire::FailureKind::Transient => RunFailureKind::Transient,
        wire::FailureKind::Permanent => RunFailureKind::Permanent,
    }
}

fn object_or_empty(value: &serde_json::Value) -> serde_json::Value {
    if value.is_object() {
        value.clone()
    } else {
        serde_json::Value::Object(serde_json::Map::new())
    }
}

async fn withdraw_stop(jobs: &Jobs, run_id: uuid::Uuid) {
    let Ok(run_id) = RunId::new(run_id) else {
        return;
    };
    if let Err(error) = jobs.transport.withdraw_stop(run_id).await {
        tracing::warn!(error = %error, "removing a desired-state cancel entry failed");
    }
}

async fn apply<F>(jobs: &Jobs, run_id: uuid::Uuid, decide: F) -> Result<(), ServiceError>
where
    F: Fn(&bc_jobs::domain::job::Job) -> Result<JobCommandResult, JobsError>,
{
    let Some(job_id) = jobs.store.job_of_run(run_id).await? else {
        tracing::debug!(run_id = %run_id, "status fact for an unknown run, discarded");
        return Ok(());
    };
    let Some(job) = jobs.load(job_id).await? else {
        return Ok(());
    };
    let result = decide(&job).unwrap_or_else(|refusal| {
        tracing::info!(
            job_id = %job_id.as_uuid(),
            run_id = %run_id,
            code = refusal.code(),
            "runner status fact refused by the domain, acknowledged and discarded"
        );
        CommandResult::new(vec![])
    });
    jobs.commit(
        vec![JobChange::new(job_id, Some(job), result.events)],
        &service_metadata(),
    )
    .await
}
