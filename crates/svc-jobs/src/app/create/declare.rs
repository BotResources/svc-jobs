use bc_jobs::JobsError;
use bc_jobs::commands::job::create::CreateJob;
use bc_jobs::domain::attempts::MaxAttempts;
use bc_jobs::domain::config::RunnerConfig;
use bc_jobs::domain::ids::{JobId, SourceEntityId};
use bc_jobs::domain::job::JobCall;
use bc_jobs::domain::keys::{DisplayName, ProducerKey, RunnerTypeKey};
use bc_jobs::domain::passport::{JobPassport, PassportSubject};
use bc_jobs::domain::payload as domain_payload;
use bc_jobs::domain::priority::Priority;
use bc_jobs::domain::references::KnownUser;
use bc_jobs::domain::requester::Requester;
use br_core_auth::Passport;
use br_core_events::UserId;
use contract_jobs::command::{CreateJob as WireV1, CreateJobV2};
use contract_jobs::command_v3::{CallMode, CreateJobV3, InlineAttachment, Payload};
use serde_json::Value;

pub fn from_v1(wire: &WireV1) -> CreateJobV3 {
    CreateJobV3 {
        job_id: wire.job_id,
        runner_type: wire.runner_type.clone(),
        producer: wire.producer.clone(),
        config: wire.config.clone(),
        payload: None,
        passport: None,
        triggered_by: wire.triggered_by.clone(),
        parent_job_id: wire.parent_job_id,
        initial_job_id: None,
        mode: None,
        caller_address: None,
        acting_run_id: None,
        acting_instance_key: None,
        source_bc: wire.source_bc.clone(),
        source_entity_id: wire.source_entity_id,
        max_attempts: wire.max_attempts,
        priority: None,
    }
}

pub fn from_v2(wire: &CreateJobV2) -> CreateJobV3 {
    CreateJobV3 {
        priority: wire.priority,
        ..from_v1(&wire.declaration())
    }
}

pub fn requested_priority(wire: &CreateJobV3) -> Result<Priority, JobsError> {
    wire.priority.map_or(Ok(Priority::DEFAULT), Priority::new)
}

pub fn build(
    wire: &CreateJobV3,
    requested_priority: Priority,
    requester: Requester,
) -> Result<CreateJob, JobsError> {
    let producer = ProducerKey::new(&wire.producer)?;
    let source_entity_id = source_entity(wire, &producer)?;
    Ok(CreateJob {
        id: JobId::new(wire.job_id)?,
        runner_type: RunnerTypeKey::new(&wire.runner_type)?,
        producer,
        config: wire.config.clone().map(RunnerConfig::new).transpose()?,
        parent_job_id: wire.parent_job_id.map(JobId::new).transpose()?,
        triggered_by: triggered_by(wire)?,
        source_entity_id,
        max_attempts: wire.max_attempts.map(max_attempts).transpose()?,
        requested_priority,
        initial_job_id: wire.initial_job_id.map(JobId::new).transpose()?,
        requester,
        passport: wire
            .passport
            .as_ref()
            .map(|document| passport(wire, document))
            .transpose()?,
        payload: wire.payload.as_ref().map(payload),
        call: wire.mode.map(|mode| call(wire, mode)),
    })
}

fn source_entity(
    wire: &CreateJobV3,
    producer: &ProducerKey,
) -> Result<Option<SourceEntityId>, JobsError> {
    match (&wire.source_bc, wire.source_entity_id) {
        (Some(bc), Some(entity_id)) => {
            if &ProducerKey::new(bc)? != producer {
                return Err(JobsError::CorruptState {
                    reason_code: "source_bc_is_not_the_producer",
                });
            }
            Ok(Some(SourceEntityId::new(entity_id)?))
        }
        (None, None) => Ok(None),
        _ => Err(JobsError::BlankValue {
            field: "source_entity_id",
        }),
    }
}

fn max_attempts(declared: i64) -> Result<MaxAttempts, JobsError> {
    let requested = u32::try_from(declared).map_err(|_| JobsError::OutOfRange {
        field: "max_attempts",
        value: declared,
    })?;
    MaxAttempts::new(requested)
}

fn triggered_by(wire: &CreateJobV3) -> Result<Option<KnownUser>, JobsError> {
    let Some(user) = &wire.triggered_by else {
        return Ok(None);
    };
    let display_name = user
        .display_name()
        .map(str::to_owned)
        .unwrap_or_else(|| user.id().to_string());
    KnownUser::new(UserId(user.id()), DisplayName::new(display_name)?).map(Some)
}

fn passport(wire: &CreateJobV3, document: &Value) -> Result<JobPassport, JobsError> {
    let parsed: Passport =
        serde_json::from_value(document.clone()).map_err(|_| JobsError::PassportMalformed)?;
    let subject = match parsed {
        Passport::Human { user_id, .. } => PassportSubject::User(user_id),
        Passport::Service {
            service_account_id, ..
        } => PassportSubject::Service(service_account_id),
    };
    let display_name = wire
        .triggered_by
        .as_ref()
        .and_then(|user| user.display_name())
        .map(str::to_owned);
    JobPassport::new(subject, display_name, document.clone())
}

fn payload(wire: &Payload) -> domain_payload::JobPayload {
    match wire {
        Payload::Inline {
            message,
            attachments,
        } => domain_payload::JobPayload::Inline {
            message: message.clone(),
            attachments: attachments.iter().map(attachment).collect(),
        },
        Payload::Kept => domain_payload::JobPayload::Kept,
        Payload::Reference {
            service,
            document_id,
        } => domain_payload::JobPayload::Reference {
            service: service.clone(),
            document_id: document_id.clone(),
        },
    }
}

fn attachment(wire: &InlineAttachment) -> domain_payload::PayloadAttachment {
    domain_payload::PayloadAttachment {
        name: wire.name.clone(),
        content_type: wire.content_type.clone(),
        content: wire.content.clone(),
    }
}

fn call(wire: &CreateJobV3, mode: CallMode) -> JobCall {
    JobCall {
        mode: match mode {
            CallMode::Sync => domain_payload::CallMode::Sync,
            CallMode::Background => domain_payload::CallMode::Background,
        },
        caller_address: wire.caller_address,
    }
}
