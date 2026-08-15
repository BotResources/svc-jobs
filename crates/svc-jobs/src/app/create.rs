use bc_jobs::JobsError;
use bc_jobs::commands::job::create::{CreateJob, CreateOutcome, create_job};
use bc_jobs::domain::attempts::MaxAttempts;
use bc_jobs::domain::config::RunnerConfig;
use bc_jobs::domain::ids::{JobId, SourceEntityId};
use bc_jobs::domain::job::Job;
use bc_jobs::domain::keys::{DisplayName, ProducerKey, RunnerTypeKey};
use bc_jobs::domain::references::KnownUser;
use bc_jobs::ports::job::JobReader;
use br_core_events::{EventMetadata, UserId};
use contract_jobs::command::CreateJob as Wire;
use contract_jobs::event::{
    REASON_DUPLICATE_ACTIVE_ENTITY, REASON_ID_REUSE, REASON_MALFORMED_PAYLOAD,
};
use serde_json::{Value, json};

use super::{Jobs, write};
use crate::error::ServiceError;

const UNATTRIBUTED_PRODUCER: &str = "unattributed";

pub async fn handle(
    jobs: &Jobs,
    wire: &Wire,
    metadata: &EventMetadata,
) -> Result<(), ServiceError> {
    match build(wire) {
        Err(refusal) => reject(jobs, wire, &refusal, metadata).await,
        Ok(declaration) => decide(jobs, wire, declaration, metadata).await,
    }
}

async fn decide(
    jobs: &Jobs,
    wire: &Wire,
    declaration: Declaration,
    metadata: &EventMetadata,
) -> Result<(), ServiceError> {
    let existing = JobReader::load(&jobs.store, declaration.id).await?;
    let parent = match declaration.parent_job_id {
        Some(parent) => JobReader::load(&jobs.store, parent).await?,
        None => None,
    };
    let command = match attributed(declaration, parent.as_ref()) {
        Ok(command) => command,
        Err(refusal) => return reject(jobs, wire, &refusal, metadata).await,
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
            let change = write::JobChange::new(command.id, None, result.events)
                .claiming_source(command.source());
            match jobs.commit(vec![change], metadata).await {
                Err(ServiceError::Domain(refusal @ JobsError::SourceAlreadyActive { .. })) => {
                    reject(jobs, wire, &refusal, metadata).await
                }
                settled => settled,
            }
        }
        Err(refusal) => reject(jobs, wire, &refusal, metadata).await,
    }
}

pub async fn reject_malformed(
    jobs: &Jobs,
    job_id: uuid::Uuid,
    metadata: &EventMetadata,
) -> Result<(), ServiceError> {
    write::publish_rejection(
        &jobs.store,
        job_id,
        REASON_MALFORMED_PAYLOAD,
        json!({ "jobId": job_id }),
        metadata,
        jobs.clock.now(),
    )
    .await
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

struct Declaration {
    id: JobId,
    runner_type: RunnerTypeKey,
    declared_producer: Option<ProducerKey>,
    config: Option<RunnerConfig>,
    parent_job_id: Option<JobId>,
    triggered_by: Option<KnownUser>,
    source_entity_id: Option<SourceEntityId>,
    max_attempts: Option<MaxAttempts>,
}

fn build(wire: &Wire) -> Result<Declaration, JobsError> {
    let declared_producer = wire.declared_producer().map(ProducerKey::new).transpose()?;
    let source_entity_id = match (&wire.source_bc, wire.source_entity_id) {
        (Some(bc), Some(entity_id)) => {
            let declared = ProducerKey::new(bc)?;
            if Some(&declared) != declared_producer.as_ref() {
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
    Ok(Declaration {
        id: JobId::new(wire.job_id)?,
        runner_type: RunnerTypeKey::new(&wire.runner_type)?,
        declared_producer,
        config: wire.config.clone().map(RunnerConfig::new).transpose()?,
        parent_job_id: wire.parent_job_id.map(JobId::new).transpose()?,
        triggered_by: triggered_by(wire)?,
        source_entity_id,
        max_attempts: wire.max_attempts.map(max_attempts).transpose()?,
    })
}

fn attributed(declaration: Declaration, parent: Option<&Job>) -> Result<CreateJob, JobsError> {
    let producer = match declaration.declared_producer {
        Some(producer) => producer,
        None => producer_of(parent)?,
    };
    Ok(CreateJob {
        id: declaration.id,
        runner_type: declaration.runner_type,
        producer,
        config: declaration.config,
        parent_job_id: declaration.parent_job_id,
        triggered_by: declaration.triggered_by,
        source_entity_id: declaration.source_entity_id,
        max_attempts: declaration.max_attempts,
    })
}

fn producer_of(parent: Option<&Job>) -> Result<ProducerKey, JobsError> {
    match parent {
        Some(parent) => ProducerKey::new(parent.runner_type().as_str()),
        None => ProducerKey::new(UNATTRIBUTED_PRODUCER),
    }
}

fn max_attempts(declared: i64) -> Result<MaxAttempts, JobsError> {
    let requested = u32::try_from(declared).map_err(|_| JobsError::OutOfRange {
        field: "max_attempts",
        value: declared,
    })?;
    MaxAttempts::new(requested)
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
