use bc_jobs::JobsError;
use bc_jobs::commands::job::create::{CreateJob, CreateOutcome, create_job};
use bc_jobs::domain::attempts::MaxAttempts;
use bc_jobs::domain::config::RunnerConfig;
use bc_jobs::domain::ids::{JobId, SourceEntityId};
use bc_jobs::domain::keys::{DisplayName, ProducerKey, RunnerTypeKey};
use bc_jobs::domain::references::KnownUser;
use bc_jobs::ports::job::JobReader;
use br_core_events::{EventMetadata, UserId};
use contract_jobs::command::CreateJob as Wire;
use contract_jobs::event::{REASON_DUPLICATE_ACTIVE_ENTITY, REASON_ID_REUSE};
use serde_json::{Value, json};

use super::{Jobs, write};
use crate::error::ServiceError;

pub async fn handle(
    jobs: &Jobs,
    wire: &Wire,
    metadata: &EventMetadata,
) -> Result<(), ServiceError> {
    match build(wire) {
        Err(refusal) => reject(jobs, wire, &refusal, metadata).await,
        Ok(command) => decide(jobs, wire, command, metadata).await,
    }
}

async fn decide(
    jobs: &Jobs,
    wire: &Wire,
    command: CreateJob,
    metadata: &EventMetadata,
) -> Result<(), ServiceError> {
    let existing = JobReader::load(&jobs.store, command.id).await?;
    let parent = match command.parent_job_id {
        Some(parent) => JobReader::load(&jobs.store, parent).await?,
        None => None,
    };
    let active = match command.source() {
        Some(source) => JobReader::load_active_for_source(&jobs.store, &source).await?,
        None => None,
    };
    let outcome = create_job(
        command.clone(),
        existing.as_ref(),
        active.as_ref(),
        parent.as_ref(),
        &jobs.limits,
    );
    match outcome {
        Ok(CreateOutcome::AlreadyQueued) => Ok(()),
        Ok(CreateOutcome::Queued(result)) => {
            jobs.commit(
                vec![write::JobChange::new(command.id, None, result.events)],
                metadata,
            )
            .await
        }
        Err(refusal) => reject(jobs, wire, &refusal, metadata).await,
    }
}

async fn reject(
    jobs: &Jobs,
    wire: &Wire,
    refusal: &JobsError,
    metadata: &EventMetadata,
) -> Result<(), ServiceError> {
    write::publish_rejection(
        &jobs.store,
        wire.job_id,
        published_code(refusal),
        rejection_params(wire, refusal),
        metadata,
        jobs.clock.now(),
    )
    .await
}

fn published_code(refusal: &JobsError) -> &'static str {
    match refusal {
        JobsError::JobIdConflict { .. } => REASON_ID_REUSE,
        JobsError::SourceAlreadyActive { .. } => REASON_DUPLICATE_ACTIVE_ENTITY,
        other => other.code(),
    }
}

fn rejection_params(wire: &Wire, refusal: &JobsError) -> Value {
    let mut params = refusal.params();
    if let Some(fields) = params.as_object_mut() {
        fields.insert("jobId".to_owned(), json!(wire.job_id));
        if let Some(entity_id) = wire.source_entity_id {
            fields.insert("sourceEntityId".to_owned(), json!(entity_id));
        }
        if let Some(bc) = &wire.source_bc {
            fields.insert("sourceBc".to_owned(), json!(bc));
        }
    }
    params
}

fn build(wire: &Wire) -> Result<CreateJob, JobsError> {
    let producer = ProducerKey::new(&wire.producer)?;
    let source_entity_id = match (&wire.source_bc, wire.source_entity_id) {
        (Some(bc), Some(entity_id)) => {
            let declared = ProducerKey::new(bc)?;
            if declared != producer {
                return Err(JobsError::CorruptState {
                    reason_code: "source_bc_is_not_the_producer",
                });
            }
            Some(SourceEntityId::new(entity_id)?)
        }
        (None, None) => None,
        _ => {
            return Err(JobsError::BlankValue {
                field: "source_entity_id",
            });
        }
    };
    Ok(CreateJob {
        id: JobId::new(wire.job_id)?,
        runner_type: RunnerTypeKey::new(&wire.runner_type)?,
        producer,
        config: wire.config.clone().map(RunnerConfig::new).transpose()?,
        parent_job_id: wire.parent_job_id.map(JobId::new).transpose()?,
        triggered_by: triggered_by(wire)?,
        source_entity_id,
        max_attempts: wire
            .max_attempts
            .map(|declared| MaxAttempts::new(u32::try_from(declared).unwrap_or(0)))
            .transpose()?,
    })
}

fn triggered_by(wire: &Wire) -> Result<Option<KnownUser>, JobsError> {
    let Some(user) = &wire.triggered_by else {
        return Ok(None);
    };
    let display_name = user
        .display_name()
        .map(str::to_owned)
        .unwrap_or_else(|| user.id().to_string());
    KnownUser::new(UserId(user.id()), DisplayName::new(display_name)?).map(Some)
}
