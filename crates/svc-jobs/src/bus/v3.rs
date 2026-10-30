use std::future::Future;
use std::sync::Arc;

use br_core_integration::{CommandCoords, EventMetadata, MessageOutcome};
use br_util_nats_fabric::Fabric;
use contract_jobs::command_v3::{CancelJobV3, CreateJobV3, FailJobV3, FinishJobV3};
use serde::de::DeserializeOwned;
use serde_json::Value;

use super::{acknowledge, declared_job_id, settle};
use crate::app::{Jobs, create, resolve_v3};
use crate::error::ServiceError;
use crate::supervision::{Established, Supervisor};

const CREATE_DURABLE: &str = "svc_jobs_job_create_v3";
const CANCEL_DURABLE: &str = "svc_jobs_job_cancel_v3";
const FINISH_DURABLE: &str = "svc_jobs_job_finish_v3";
const FAIL_DURABLE: &str = "svc_jobs_job_fail_v3";

pub async fn verify_durables(fabric: &Fabric) -> Result<(), ServiceError> {
    fabric
        .ensure_command_consumer::<Value>(
            &contract_jobs::cmd_job_create_v3_coords()?,
            CREATE_DURABLE,
        )
        .await?;
    fabric
        .ensure_command_consumer::<CancelJobV3>(
            &contract_jobs::cmd_job_cancel_v3_coords()?,
            CANCEL_DURABLE,
        )
        .await?;
    fabric
        .ensure_command_consumer::<FinishJobV3>(
            &contract_jobs::cmd_job_finish_v3_coords()?,
            FINISH_DURABLE,
        )
        .await?;
    fabric
        .ensure_command_consumer::<FailJobV3>(
            &contract_jobs::cmd_job_fail_v3_coords()?,
            FAIL_DURABLE,
        )
        .await?;
    Ok(())
}

pub fn spawn(supervisor: &Supervisor, fabric: &Fabric, jobs: &Arc<Jobs>) {
    let (f, j) = (fabric.clone(), Arc::clone(jobs));
    supervisor.spawn("job-creation-v3 consumer", move |established| {
        let (fabric, jobs) = (f.clone(), Arc::clone(&j));
        async move { consume_creations(fabric, jobs, established).await }
    });
    let (f, j) = (fabric.clone(), Arc::clone(jobs));
    supervisor.spawn("job-cancellation-v3 consumer", move |established| {
        let (fabric, jobs) = (f.clone(), Arc::clone(&j));
        async move { consume_cancellations(fabric, jobs, established).await }
    });
    let (f, j) = (fabric.clone(), Arc::clone(jobs));
    supervisor.spawn("job-completion-v3 consumer", move |established| {
        let (fabric, jobs) = (f.clone(), Arc::clone(&j));
        async move { consume_completions(fabric, jobs, established).await }
    });
    let (f, j) = (fabric.clone(), Arc::clone(jobs));
    supervisor.spawn("job-failure-v3 consumer", move |established| {
        let (fabric, jobs) = (f.clone(), Arc::clone(&j));
        async move { consume_failures(fabric, jobs, established).await }
    });
}

async fn consume_creations(
    fabric: Fabric,
    jobs: Arc<Jobs>,
    established: Established,
) -> Result<(), ServiceError> {
    run(
        fabric,
        contract_jobs::cmd_job_create_v3_coords()?,
        CREATE_DURABLE,
        established,
        |payload: Value, metadata| {
            let jobs = Arc::clone(&jobs);
            async move { create_outcome(&jobs, payload, &metadata).await }
        },
    )
    .await
}

async fn create_outcome(jobs: &Jobs, payload: Value, metadata: &EventMetadata) -> MessageOutcome {
    match serde_json::from_value::<CreateJobV3>(payload.clone()) {
        Ok(command) => settle(create::handle_v3(jobs, &command, metadata).await),
        Err(error) => {
            let Some(job_id) = declared_job_id(&payload) else {
                tracing::warn!(error = %error, "a job creation carried an unreadable payload and no job id, discarded");
                return MessageOutcome::Term;
            };
            tracing::warn!(error = %error, job_id = %job_id, "a job creation carried an unreadable payload, answered by a rejection");
            settle(create::reject_malformed(jobs, job_id, metadata).await)
        }
    }
}

async fn consume_cancellations(
    fabric: Fabric,
    jobs: Arc<Jobs>,
    established: Established,
) -> Result<(), ServiceError> {
    run(
        fabric,
        contract_jobs::cmd_job_cancel_v3_coords()?,
        CANCEL_DURABLE,
        established,
        |payload: CancelJobV3, metadata| {
            let jobs = Arc::clone(&jobs);
            async move { settle(resolve_v3::cancel(&jobs, &payload, &metadata).await) }
        },
    )
    .await
}

async fn consume_completions(
    fabric: Fabric,
    jobs: Arc<Jobs>,
    established: Established,
) -> Result<(), ServiceError> {
    run(
        fabric,
        contract_jobs::cmd_job_finish_v3_coords()?,
        FINISH_DURABLE,
        established,
        |payload: FinishJobV3, metadata| {
            let jobs = Arc::clone(&jobs);
            async move { settle(resolve_v3::finish(&jobs, &payload, &metadata).await) }
        },
    )
    .await
}

async fn consume_failures(
    fabric: Fabric,
    jobs: Arc<Jobs>,
    established: Established,
) -> Result<(), ServiceError> {
    run(
        fabric,
        contract_jobs::cmd_job_fail_v3_coords()?,
        FAIL_DURABLE,
        established,
        |payload: FailJobV3, metadata| {
            let jobs = Arc::clone(&jobs);
            async move { settle(resolve_v3::fail(&jobs, &payload, &metadata).await) }
        },
    )
    .await
}

async fn run<T, Fut>(
    fabric: Fabric,
    coords: CommandCoords,
    durable: &'static str,
    established: Established,
    handle: impl Fn(T, EventMetadata) -> Fut,
) -> Result<(), ServiceError>
where
    T: DeserializeOwned + Clone,
    Fut: Future<Output = MessageOutcome>,
{
    let mut consumer = fabric
        .ensure_command_consumer::<T>(&coords, durable)
        .await?;
    established.signal();
    while let Some(delivery) = consumer.recv().await? {
        let outcome = match delivery.payload() {
            Err(_) => MessageOutcome::Term,
            Ok(command) => {
                let metadata = command.metadata.clone().with_causation(command.command_id);
                handle(command.payload.clone(), metadata).await
            }
        };
        acknowledge(delivery, outcome).await;
    }
    Ok(())
}
