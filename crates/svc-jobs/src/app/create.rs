use bc_jobs::JobsError;
use bc_jobs::commands::job::create::{CreateJob, CreateOutcome, create_job};
use bc_jobs::domain::ids::KnownUserId;
use bc_jobs::domain::ownership::ActorRef;
use bc_jobs::domain::priority::PriorityBasis;
use bc_jobs::domain::requester::Requester;
use bc_jobs::ports::fleet::FleetReader;
use bc_jobs::ports::job::JobReader;
use bc_jobs::ports::user::UserMaxPriorityReader;
use br_core_events::EventMetadata;
use contract_jobs::command::{CreateJob as WireV1, CreateJobV2};
use contract_jobs::command_v3::CreateJobV3;
use contract_jobs::event::{
    REASON_DUPLICATE_ACTIVE_ENTITY, REASON_ID_REUSE, REASON_MALFORMED_PAYLOAD,
};
use serde_json::{Value, json};

mod declare;

use super::integration::Lineage;
use super::write::{CreationRefusal, PriorityPremise};
use super::{Jobs, acting, write};
use crate::error::ServiceError;

pub async fn handle(
    jobs: &Jobs,
    wire: &WireV1,
    metadata: &EventMetadata,
) -> Result<(), ServiceError> {
    admit(jobs, declare::from_v1(wire), metadata).await
}

pub async fn handle_v2(
    jobs: &Jobs,
    wire: &CreateJobV2,
    metadata: &EventMetadata,
) -> Result<(), ServiceError> {
    admit(jobs, declare::from_v2(wire), metadata).await
}

pub async fn handle_v3(
    jobs: &Jobs,
    wire: &CreateJobV3,
    metadata: &EventMetadata,
) -> Result<(), ServiceError> {
    admit(jobs, wire.clone(), metadata).await
}

async fn admit(
    jobs: &Jobs,
    wire: CreateJobV3,
    metadata: &EventMetadata,
) -> Result<(), ServiceError> {
    let (wire, requester) = match requester_of(jobs, wire.clone(), metadata).await {
        Ok(found) => found,
        Err(ServiceError::Domain(refusal)) => return reject(jobs, &wire, &refusal, metadata).await,
        Err(other) => return Err(other),
    };
    let built = declare::requested_priority(&wire)
        .and_then(|requested| declare::build(&wire, requested, requester));
    match built {
        Err(refusal) => reject(jobs, &wire, &refusal, metadata).await,
        Ok(command) => decide(jobs, &wire, command, metadata).await,
    }
}

async fn requester_of(
    jobs: &Jobs,
    mut wire: CreateJobV3,
    metadata: &EventMetadata,
) -> Result<(CreateJobV3, Requester), ServiceError> {
    let acting = acting::named(
        jobs,
        wire.acting_run_id,
        wire.acting_instance_key.as_deref(),
    )
    .await?;
    let Some(acting) = acting else {
        let actor = ActorRef::new(metadata.actor.id());
        return Ok((wire, Requester::Service(Some(actor))));
    };
    let run_job = acting.job_id().as_uuid();
    match wire.parent_job_id {
        None => wire.parent_job_id = Some(run_job),
        Some(given) if given != run_job => return Err(JobsError::ParentNotOfRun.into()),
        Some(_) => {}
    }
    Ok((wire, Requester::Run(acting.run_id)))
}

async fn decide(
    jobs: &Jobs,
    wire: &CreateJobV3,
    command: CreateJob,
    metadata: &EventMetadata,
) -> Result<(), ServiceError> {
    if let Some(runner_type) = FleetReader::load(&jobs.store, &command.runner_type).await?
        && let Err(refusal) = runner_type.guard_accept_job()
    {
        return reject(jobs, wire, &refusal, metadata).await;
    }
    let existing = JobReader::load(&jobs.store, command.id).await?;
    let parent = match command.parent_job_id {
        Some(parent) => JobReader::load(&jobs.store, parent).await?,
        None => None,
    };
    let active = match command.source() {
        Some(source) => JobReader::load_active_for_source(&jobs.store, &source).await?,
        None => None,
    };
    let user_maximum = match &command.triggered_by {
        Some(user) => Some(
            UserMaxPriorityReader::load_one(&jobs.store, KnownUserId::new(user.id().0)?).await?,
        ),
        None => None,
    };
    let outcome = create_job(
        command.clone(),
        existing.as_ref(),
        active.as_ref(),
        parent.as_ref(),
        user_maximum.as_ref(),
        &jobs.limits,
    );
    match outcome {
        Ok(CreateOutcome::AlreadyQueued) => Ok(()),
        Ok(CreateOutcome::Queued(result)) => {
            let admitted_under = command.parent_job_id.and(parent.as_ref());
            let premise = PriorityPremise::of(
                PriorityBasis::of(admitted_under, user_maximum.as_ref(), &jobs.limits),
                admitted_under,
                user_maximum.as_ref(),
            );
            let change = write::JobChange::new(command.id, None, result.events)
                .claiming_source(command.source())
                .on_priority_premise(premise);
            match jobs.commit(vec![change], metadata).await {
                Err(ServiceError::Domain(
                    refusal @ (JobsError::SourceAlreadyActive { .. }
                    | JobsError::RunnerTypeRetired { .. }
                    | JobsError::ParentJobTerminal { .. }
                    | JobsError::ParentJobDeleted { .. }
                    | JobsError::ParentJobUnknown { .. }),
                )) => reject(jobs, wire, &refusal, metadata).await,
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
        jobs.ids.as_ref(),
        CreationRefusal {
            job_id,
            lineage: Lineage::declared(job_id, None, None),
            reason_code: REASON_MALFORMED_PAYLOAD,
            params: json!({ "jobId": job_id }),
        },
        metadata,
        jobs.clock.now(),
    )
    .await
}

async fn reject(
    jobs: &Jobs,
    wire: &CreateJobV3,
    refusal: &JobsError,
    metadata: &EventMetadata,
) -> Result<(), ServiceError> {
    write::publish_rejection(
        &jobs.store,
        jobs.ids.as_ref(),
        CreationRefusal {
            job_id: wire.job_id,
            lineage: Lineage::declared(wire.job_id, wire.parent_job_id, wire.initial_job_id),
            reason_code: published_code(refusal),
            params: rejection_params(wire, refusal),
        },
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

fn rejection_params(wire: &CreateJobV3, refusal: &JobsError) -> Value {
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
