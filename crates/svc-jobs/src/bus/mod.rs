use std::sync::Arc;

use bc_jobs::domain::ids::{JobId, ResolutionId};
use bc_jobs::domain::ownership::CancelRequester;
use br_core_integration::MessageOutcome;
use br_util_nats_fabric::{CommandConsumer, Fabric};
use contract_jobs::command as wire;
use serde::de::DeserializeOwned;
use serde_json::Value;
use uuid::Uuid;

use crate::app::{Jobs, create, resolve};
use crate::error::ServiceError;

const CREATE_DURABLE: &str = "svc_jobs_job_create";
const CANCEL_DURABLE: &str = "svc_jobs_job_cancel";
const FINISH_DURABLE: &str = "svc_jobs_job_finish";
const FAIL_DURABLE: &str = "svc_jobs_job_fail";

pub async fn verify_durables(fabric: &Fabric) -> Result<(), ServiceError> {
    open::<Value>(fabric, Verb::Create).await?;
    open::<wire::CancelJob>(fabric, Verb::Cancel).await?;
    open::<wire::FinishJob>(fabric, Verb::Finish).await?;
    open::<wire::FailJob>(fabric, Verb::Fail).await?;
    Ok(())
}

#[derive(Clone, Copy)]
pub enum Verb {
    Create,
    Cancel,
    Finish,
    Fail,
}

impl Verb {
    fn durable(self) -> &'static str {
        match self {
            Self::Create => CREATE_DURABLE,
            Self::Cancel => CANCEL_DURABLE,
            Self::Finish => FINISH_DURABLE,
            Self::Fail => FAIL_DURABLE,
        }
    }

    fn coords(self) -> Result<br_core_integration::CommandCoords, ServiceError> {
        let coords = match self {
            Self::Create => contract_jobs::cmd_job_create_v1_coords(),
            Self::Cancel => contract_jobs::cmd_job_cancel_v1_coords(),
            Self::Finish => contract_jobs::cmd_job_finish_v1_coords(),
            Self::Fail => contract_jobs::cmd_job_fail_v1_coords(),
        };
        coords.map_err(|error| ServiceError::Infra(error.to_string()))
    }
}

async fn open<T: DeserializeOwned>(
    fabric: &Fabric,
    verb: Verb,
) -> Result<CommandConsumer<T>, ServiceError> {
    fabric
        .ensure_command_consumer::<T>(&verb.coords()?, verb.durable())
        .await
        .map_err(ServiceError::from)
}

pub async fn consume_creations(fabric: Fabric, jobs: Arc<Jobs>) -> Result<(), ServiceError> {
    let mut consumer = open::<Value>(&fabric, Verb::Create).await?;
    while let Some(delivery) = consumer.recv().await? {
        let outcome = match delivery.payload() {
            Err(_) => MessageOutcome::Term,
            Ok(command) => {
                let payload = command.payload.clone();
                let metadata = command.metadata.clone();
                create_outcome(&jobs, payload, &metadata).await
            }
        };
        acknowledge(delivery, outcome).await;
    }
    Ok(())
}

async fn create_outcome(
    jobs: &Jobs,
    payload: Value,
    metadata: &br_core_integration::EventMetadata,
) -> MessageOutcome {
    match serde_json::from_value::<wire::CreateJob>(payload.clone()) {
        Ok(command) => settle(create::handle(jobs, &command, metadata).await),
        Err(error) => {
            let Some(job_id) = declared_job_id(&payload) else {
                tracing::warn!(
                    error = %error,
                    "a job creation carried an unreadable payload and no job id, discarded"
                );
                return MessageOutcome::Term;
            };
            tracing::warn!(
                error = %error,
                job_id = %job_id,
                "a job creation carried an unreadable payload, answered by a rejection"
            );
            settle(create::reject_malformed(jobs, job_id, metadata).await)
        }
    }
}

fn declared_job_id(payload: &Value) -> Option<Uuid> {
    payload
        .get("job_id")
        .and_then(Value::as_str)
        .and_then(|raw| Uuid::parse_str(raw).ok())
}

pub async fn consume_cancellations(fabric: Fabric, jobs: Arc<Jobs>) -> Result<(), ServiceError> {
    let mut consumer = open::<wire::CancelJob>(&fabric, Verb::Cancel).await?;
    while let Some(delivery) = consumer.recv().await? {
        let outcome = match delivery.payload() {
            Err(_) => MessageOutcome::Term,
            Ok(command) => {
                let payload = command.payload.clone();
                let metadata = command.metadata.clone();
                settle(cancel(&jobs, &payload, &metadata).await)
            }
        };
        acknowledge(delivery, outcome).await;
    }
    Ok(())
}

async fn cancel(
    jobs: &Jobs,
    payload: &wire::CancelJob,
    metadata: &br_core_integration::EventMetadata,
) -> Result<(), ServiceError> {
    let job_id = JobId::new(payload.job_id)?;
    let job = jobs.require(job_id).await?;
    let claim = resolve::owner_claim(jobs, &job, metadata).await?;
    resolve::cancel(
        jobs,
        job_id,
        ResolutionId::new(jobs.ids.next())?,
        CancelRequester::Owner(claim),
        metadata,
    )
    .await
}

pub async fn consume_completions(fabric: Fabric, jobs: Arc<Jobs>) -> Result<(), ServiceError> {
    let mut consumer = open::<wire::FinishJob>(&fabric, Verb::Finish).await?;
    while let Some(delivery) = consumer.recv().await? {
        let outcome = match delivery.payload() {
            Err(_) => MessageOutcome::Term,
            Ok(command) => {
                let payload = command.payload.clone();
                let metadata = command.metadata.clone();
                settle(finish(&jobs, &payload, &metadata).await)
            }
        };
        acknowledge(delivery, outcome).await;
    }
    Ok(())
}

async fn finish(
    jobs: &Jobs,
    payload: &wire::FinishJob,
    metadata: &br_core_integration::EventMetadata,
) -> Result<(), ServiceError> {
    resolve::finish(
        jobs,
        JobId::new(payload.job_id)?,
        ResolutionId::new(jobs.ids.next())?,
        metadata,
    )
    .await
}

pub async fn consume_failures(fabric: Fabric, jobs: Arc<Jobs>) -> Result<(), ServiceError> {
    let mut consumer = open::<wire::FailJob>(&fabric, Verb::Fail).await?;
    while let Some(delivery) = consumer.recv().await? {
        let outcome = match delivery.payload() {
            Err(_) => MessageOutcome::Term,
            Ok(command) => {
                let payload = command.payload.clone();
                let metadata = command.metadata.clone();
                settle(fail(&jobs, &payload, &metadata).await)
            }
        };
        acknowledge(delivery, outcome).await;
    }
    Ok(())
}

async fn fail(
    jobs: &Jobs,
    payload: &wire::FailJob,
    metadata: &br_core_integration::EventMetadata,
) -> Result<(), ServiceError> {
    resolve::fail(
        jobs,
        JobId::new(payload.job_id)?,
        ResolutionId::new(jobs.ids.next())?,
        payload.note.clone(),
        metadata,
    )
    .await
}

fn settle(outcome: Result<(), ServiceError>) -> MessageOutcome {
    match outcome {
        Ok(()) => MessageOutcome::Ack,
        Err(ServiceError::Infra(detail)) => {
            tracing::error!(detail, "an integration command failed on infrastructure");
            MessageOutcome::Nak(None)
        }
        Err(ServiceError::Contended) => {
            tracing::debug!("an integration command lost a write race, redelivered");
            MessageOutcome::Nak(None)
        }
        Err(refused) => {
            tracing::info!(
                error = %refused,
                "an integration command was refused by the domain, acknowledged and discarded"
            );
            MessageOutcome::Ack
        }
    }
}

async fn acknowledge<T>(delivery: br_util_nats_fabric::Delivered<T>, outcome: MessageOutcome) {
    let result = match outcome {
        MessageOutcome::Ack => delivery.ack().await,
        MessageOutcome::Nak(delay) => delivery.nak(delay).await,
        _ => delivery.term().await,
    };
    if let Err(error) = result {
        tracing::warn!(error = %error, "acknowledging an integration command failed");
    }
}
