use bc_jobs::domain::ids::{EventId, JobId, RunId, RunLogId};
use bc_jobs::event::fleet::FleetEvent;
use bc_jobs::event::job::JobEvent;
use bc_jobs::ports::PortError;
use serde::{Deserialize, Serialize};
use sqlx::PgConnection;
use uuid::Uuid;

use crate::db::hydrate::unavailable;

pub const DOMAIN_EVENT_CHANNEL: &str = "jobs_domain_event";
pub const RUN_LOG_CHANNEL: &str = "jobs_run_log";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomainEventSignal {
    pub event_id: Uuid,
    pub aggregate_type: String,
    pub event_type: String,
    pub aggregate_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunLogSignal {
    pub log_id: Uuid,
    pub job_id: Uuid,
    pub run_id: Uuid,
}

pub async fn job_event(
    tx: &mut PgConnection,
    event_id: EventId,
    event: &JobEvent,
) -> Result<(), PortError> {
    send(
        tx,
        DOMAIN_EVENT_CHANNEL,
        &DomainEventSignal {
            event_id: event_id.as_uuid(),
            aggregate_type: bc_jobs::event::job::JOB_AGGREGATE_TYPE.to_owned(),
            event_type: event.event_type().to_owned(),
            aggregate_id: event.job_id().as_uuid(),
        },
    )
    .await
}

pub async fn fleet_event(
    tx: &mut PgConnection,
    event_id: EventId,
    event: &FleetEvent,
) -> Result<(), PortError> {
    send(
        tx,
        DOMAIN_EVENT_CHANNEL,
        &DomainEventSignal {
            event_id: event_id.as_uuid(),
            aggregate_type: bc_jobs::event::fleet::RUNNER_TYPE_AGGREGATE_TYPE.to_owned(),
            event_type: event.event_type().to_owned(),
            aggregate_id: event.runner_type_id().as_uuid(),
        },
    )
    .await
}

pub async fn run_log(
    tx: &mut PgConnection,
    log_id: RunLogId,
    job_id: JobId,
    run_id: RunId,
) -> Result<(), PortError> {
    send(
        tx,
        RUN_LOG_CHANNEL,
        &RunLogSignal {
            log_id: log_id.as_uuid(),
            job_id: job_id.as_uuid(),
            run_id: run_id.as_uuid(),
        },
    )
    .await
}

async fn send<T: Serialize>(
    tx: &mut PgConnection,
    channel: &str,
    signal: &T,
) -> Result<(), PortError> {
    let payload = serde_json::to_string(signal).map_err(|error| PortError::Unavailable {
        detail: error.to_string(),
    })?;
    sqlx::query("SELECT pg_notify($1, $2)")
        .bind(channel)
        .bind(payload)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
    Ok(())
}
