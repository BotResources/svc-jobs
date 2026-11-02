use bc_jobs::JobsError;
use bc_jobs::commands::job::create::{CreateJob, CreateOutcome, create_job};
use bc_jobs::domain::directory::caller_address;
use bc_jobs::domain::ids::{KnownUserId, RunId};
use bc_jobs::domain::ownership::ActorRef;
use bc_jobs::domain::priority::PriorityBasis;
use bc_jobs::domain::requester::Requester;
use bc_jobs::domain::runner::kept_payload::guard_kept_payload_uploaded;
use bc_jobs::ports::fleet::FleetReader;
use bc_jobs::ports::job::JobReader;
use bc_jobs::ports::kept_payload::KeptPayloads;
use bc_jobs::ports::user::UserMaxPriorityReader;
use br_core_events::EventMetadata;
use contract_jobs::command::{CreateJob as WireV1, CreateJobV2};
use contract_jobs::command_v3::{CallMode, CreateJobV3};

mod declare;
mod refusal;
mod repeat;

pub use refusal::reject_malformed;
use refusal::{caller_of, reject};
use repeat::queued_again;

use super::write::{CallerAddressPremise, PriorityPremise};
use super::{Jobs, acting, write};
use crate::error::ServiceError;

pub async fn handle(
    jobs: &Jobs,
    wire: &WireV1,
    metadata: &EventMetadata,
) -> Result<(), ServiceError> {
    admit(jobs, declare::from_v1(wire), Repeats::Unanswered, metadata).await
}

pub async fn handle_v2(
    jobs: &Jobs,
    wire: &CreateJobV2,
    metadata: &EventMetadata,
) -> Result<(), ServiceError> {
    admit(jobs, declare::from_v2(wire), Repeats::Unanswered, metadata).await
}

pub async fn handle_v3(
    jobs: &Jobs,
    wire: &CreateJobV3,
    metadata: &EventMetadata,
) -> Result<(), ServiceError> {
    admit(jobs, wire.clone(), Repeats::Answered, metadata).await
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Repeats {
    Answered,
    Unanswered,
}

async fn admit(
    jobs: &Jobs,
    wire: CreateJobV3,
    repeats: Repeats,
    metadata: &EventMetadata,
) -> Result<(), ServiceError> {
    let acting = match acting::named(
        jobs,
        wire.acting_run_id,
        wire.acting_instance_key.as_deref(),
    )
    .await
    {
        Ok(acting) => acting,
        Err(ServiceError::Domain(refusal)) => {
            return reject(jobs, &wire, &refusal, metadata, None).await;
        }
        Err(other) => return Err(other),
    };
    let named_run = acting.as_ref().map(|acting| acting.run_id);
    let (wire, requester) = match requester_of(jobs, wire.clone(), acting, metadata).await {
        Ok(found) => found,
        Err(ServiceError::Domain(refusal)) => {
            return reject(jobs, &wire, &refusal, metadata, named_run).await;
        }
        Err(other) => return Err(other),
    };
    let built = declare::requested_priority(&wire)
        .and_then(|requested| declare::build(&wire, requested, requester));
    match built {
        Err(refusal) => reject(jobs, &wire, &refusal, metadata, named_run).await,
        Ok(command) => decide(jobs, &wire, command, repeats, metadata).await,
    }
}

async fn requester_of(
    jobs: &Jobs,
    mut wire: CreateJobV3,
    acting: Option<acting::ActingRun>,
    metadata: &EventMetadata,
) -> Result<(CreateJobV3, Requester), ServiceError> {
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
    guard_caller_address(jobs, &wire, acting.run_id).await?;
    Ok((wire, Requester::Run(acting.run_id)))
}

fn caller_address_premise(wire: &CreateJobV3, run_id: RunId) -> Option<CallerAddressPremise> {
    let mode = match wire.mode? {
        CallMode::Sync => bc_jobs::domain::payload::CallMode::Sync,
        CallMode::Background => bc_jobs::domain::payload::CallMode::Background,
    };
    Some(CallerAddressPremise {
        mode,
        address: wire.caller_address,
        caller: run_id,
    })
}

async fn guard_caller_address(
    jobs: &Jobs,
    wire: &CreateJobV3,
    run_id: RunId,
) -> Result<(), ServiceError> {
    let Some(premise) = caller_address_premise(wire, run_id) else {
        return Ok(());
    };
    let holder = match premise.address {
        Some(address) => jobs.store.mail_address_run(address).await?,
        None => None,
    };
    Ok(caller_address::guard_caller_address(
        premise.mode,
        premise.address,
        premise.caller,
        holder,
    )?)
}

async fn decide(
    jobs: &Jobs,
    wire: &CreateJobV3,
    command: CreateJob,
    repeats: Repeats,
    metadata: &EventMetadata,
) -> Result<(), ServiceError> {
    let caller = caller_of(&command.requester);
    let existing = JobReader::load(&jobs.store, command.id).await?;
    if repeats == Repeats::Answered
        && let Some(first) = existing
            .as_ref()
            .filter(|first| command.is_the_same_declaration_as(first))
    {
        return queued_again(jobs, first, metadata).await;
    }
    if let Some(runner_type) = FleetReader::load(&jobs.store, &command.runner_type).await?
        && let Err(refusal) = runner_type.guard_accept_job()
    {
        return reject(jobs, wire, &refusal, metadata, caller).await;
    }
    if let Some(refusal) = kept_payload_refusal(jobs, &command).await? {
        return reject(jobs, wire, &refusal, metadata, caller).await;
    }
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
            let caller_premise = match command.requester {
                Requester::Run(run_id) => caller_address_premise(wire, run_id),
                Requester::Service(_) => None,
            };
            let change = write::JobChange::new(command.id, None, result.events)
                .claiming_source(command.source())
                .holding_caller_address(caller_premise)
                .on_priority_premise(premise);
            match jobs.commit(vec![change], metadata).await {
                Err(ServiceError::Domain(
                    refusal @ (JobsError::SourceAlreadyActive { .. }
                    | JobsError::RunnerTypeRetired { .. }
                    | JobsError::ParentJobTerminal { .. }
                    | JobsError::ParentJobDeleted { .. }
                    | JobsError::ParentJobUnknown { .. }
                    | JobsError::CallerAddressNotHeld
                    | JobsError::CallerAddressNotOfRun),
                )) => reject(jobs, wire, &refusal, metadata, caller).await,
                settled => settled,
            }
        }
        Err(refusal) => reject(jobs, wire, &refusal, metadata, caller).await,
    }
}

async fn kept_payload_refusal(
    jobs: &Jobs,
    command: &CreateJob,
) -> Result<Option<JobsError>, ServiceError> {
    if !command
        .payload
        .as_ref()
        .is_some_and(|payload| payload.is_kept())
    {
        return Ok(None);
    }
    let stored = jobs.store.stored_payload(command.id.as_uuid()).await?;
    Ok(guard_kept_payload_uploaded(&command.requester, stored.as_ref()).err())
}
