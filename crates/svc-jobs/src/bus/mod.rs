pub mod runner_type_catalog;

use std::sync::Arc;

use bc_jobs::domain::ids::{JobId, ResolutionId};
use br_core_integration::MessageOutcome;
use br_util_nats_fabric::{CommandConsumer, Fabric};
use contract_jobs::command as wire;
use serde::de::DeserializeOwned;
use serde_json::Value;
use uuid::Uuid;

use crate::app::{Jobs, create, resolve};
use crate::error::ServiceError;
use crate::supervision::Established;

const CREATE_DURABLE: &str = "svc_jobs_job_create";
const CANCEL_DURABLE: &str = "svc_jobs_job_cancel";
const CANCEL_V2_DURABLE: &str = "svc_jobs_job_cancel_v2";
const FINISH_DURABLE: &str = "svc_jobs_job_finish";
const FINISH_V2_DURABLE: &str = "svc_jobs_job_finish_v2";
const FAIL_DURABLE: &str = "svc_jobs_job_fail";
const FAIL_V2_DURABLE: &str = "svc_jobs_job_fail_v2";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContractVersion {
    V1,
    V2,
}

impl ContractVersion {
    fn admission(self) -> resolve::IntegrationAdmission {
        match self {
            Self::V1 => resolve::IntegrationAdmission::LegacyOwner,
            Self::V2 => resolve::IntegrationAdmission::LifecycleOnly,
        }
    }
}

pub async fn verify_durables(fabric: &Fabric) -> Result<(), ServiceError> {
    open::<Value>(fabric, Verb::Create, ContractVersion::V1).await?;
    for version in [ContractVersion::V1, ContractVersion::V2] {
        open::<wire::CancelJob>(fabric, Verb::Cancel, version).await?;
        open::<wire::FinishJob>(fabric, Verb::Finish, version).await?;
        open::<wire::FailJob>(fabric, Verb::Fail, version).await?;
    }
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
    fn durable(self, version: ContractVersion) -> Result<&'static str, ServiceError> {
        match (self, version) {
            (Self::Create, ContractVersion::V1) => Ok(CREATE_DURABLE),
            (Self::Cancel, ContractVersion::V1) => Ok(CANCEL_DURABLE),
            (Self::Cancel, ContractVersion::V2) => Ok(CANCEL_V2_DURABLE),
            (Self::Finish, ContractVersion::V1) => Ok(FINISH_DURABLE),
            (Self::Finish, ContractVersion::V2) => Ok(FINISH_V2_DURABLE),
            (Self::Fail, ContractVersion::V1) => Ok(FAIL_DURABLE),
            (Self::Fail, ContractVersion::V2) => Ok(FAIL_V2_DURABLE),
            (Self::Create, ContractVersion::V2) => Err(ServiceError::Infra(
                "job.create.v2 is not declared".to_owned(),
            )),
        }
    }

    fn coords(
        self,
        version: ContractVersion,
    ) -> Result<br_core_integration::CommandCoords, ServiceError> {
        let coords = match (self, version) {
            (Self::Create, ContractVersion::V1) => contract_jobs::cmd_job_create_v1_coords(),
            (Self::Cancel, ContractVersion::V1) => contract_jobs::cmd_job_cancel_v1_coords(),
            (Self::Cancel, ContractVersion::V2) => contract_jobs::cmd_job_cancel_v2_coords(),
            (Self::Finish, ContractVersion::V1) => contract_jobs::cmd_job_finish_v1_coords(),
            (Self::Finish, ContractVersion::V2) => contract_jobs::cmd_job_finish_v2_coords(),
            (Self::Fail, ContractVersion::V1) => contract_jobs::cmd_job_fail_v1_coords(),
            (Self::Fail, ContractVersion::V2) => contract_jobs::cmd_job_fail_v2_coords(),
            (Self::Create, ContractVersion::V2) => {
                return Err(ServiceError::Infra(
                    "job.create.v2 is not declared".to_owned(),
                ));
            }
        };
        coords.map_err(|error| ServiceError::Infra(error.to_string()))
    }
}

async fn open<T: DeserializeOwned>(
    fabric: &Fabric,
    verb: Verb,
    version: ContractVersion,
) -> Result<CommandConsumer<T>, ServiceError> {
    fabric
        .ensure_command_consumer::<T>(&verb.coords(version)?, verb.durable(version)?)
        .await
        .map_err(ServiceError::from)
}

pub async fn consume_creations(
    fabric: Fabric,
    jobs: Arc<Jobs>,
    established: Established,
) -> Result<(), ServiceError> {
    let mut consumer = open::<Value>(&fabric, Verb::Create, ContractVersion::V1).await?;
    established.signal();
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

pub async fn consume_cancellations(
    fabric: Fabric,
    jobs: Arc<Jobs>,
    version: ContractVersion,
    established: Established,
) -> Result<(), ServiceError> {
    let mut consumer = open::<wire::CancelJob>(&fabric, Verb::Cancel, version).await?;
    established.signal();
    while let Some(delivery) = consumer.recv().await? {
        let outcome = match delivery.payload() {
            Err(_) => MessageOutcome::Term,
            Ok(command) => {
                let payload = command.payload.clone();
                let metadata = command.metadata.clone();
                settle(cancel(&jobs, &payload, version, &metadata).await)
            }
        };
        acknowledge(delivery, outcome).await;
    }
    Ok(())
}

async fn cancel(
    jobs: &Jobs,
    payload: &wire::CancelJob,
    version: ContractVersion,
    metadata: &br_core_integration::EventMetadata,
) -> Result<(), ServiceError> {
    let job_id = JobId::new(payload.job_id)?;
    resolve::cancel_from_integration(
        jobs,
        job_id,
        ResolutionId::new(jobs.ids.next())?,
        version.admission(),
        metadata,
    )
    .await
}

pub async fn consume_completions(
    fabric: Fabric,
    jobs: Arc<Jobs>,
    version: ContractVersion,
    established: Established,
) -> Result<(), ServiceError> {
    let mut consumer = open::<wire::FinishJob>(&fabric, Verb::Finish, version).await?;
    established.signal();
    while let Some(delivery) = consumer.recv().await? {
        let outcome = match delivery.payload() {
            Err(_) => MessageOutcome::Term,
            Ok(command) => {
                let payload = command.payload.clone();
                let metadata = command.metadata.clone();
                settle(finish(&jobs, &payload, version, &metadata).await)
            }
        };
        acknowledge(delivery, outcome).await;
    }
    Ok(())
}

async fn finish(
    jobs: &Jobs,
    payload: &wire::FinishJob,
    version: ContractVersion,
    metadata: &br_core_integration::EventMetadata,
) -> Result<(), ServiceError> {
    resolve::finish(
        jobs,
        JobId::new(payload.job_id)?,
        ResolutionId::new(jobs.ids.next())?,
        version.admission(),
        metadata,
    )
    .await
}

pub async fn consume_failures(
    fabric: Fabric,
    jobs: Arc<Jobs>,
    version: ContractVersion,
    established: Established,
) -> Result<(), ServiceError> {
    let mut consumer = open::<wire::FailJob>(&fabric, Verb::Fail, version).await?;
    established.signal();
    while let Some(delivery) = consumer.recv().await? {
        let outcome = match delivery.payload() {
            Err(_) => MessageOutcome::Term,
            Ok(command) => {
                let payload = command.payload.clone();
                let metadata = command.metadata.clone();
                settle(fail(&jobs, &payload, version, &metadata).await)
            }
        };
        acknowledge(delivery, outcome).await;
    }
    Ok(())
}

async fn fail(
    jobs: &Jobs,
    payload: &wire::FailJob,
    version: ContractVersion,
    metadata: &br_core_integration::EventMetadata,
) -> Result<(), ServiceError> {
    resolve::fail(
        jobs,
        JobId::new(payload.job_id)?,
        ResolutionId::new(jobs.ids.next())?,
        payload.note.clone(),
        version.admission(),
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
