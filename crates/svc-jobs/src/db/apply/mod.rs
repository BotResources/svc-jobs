pub mod fleet;
pub mod job;
pub mod refs;
pub mod run;

use bc_jobs::domain::ids::{EventId, JobId, RunnerTypeId};
use bc_jobs::event::fleet::{FleetEvent, RUNNER_TYPE_AGGREGATE_TYPE};
use bc_jobs::event::job::{JOB_AGGREGATE_TYPE, JobEvent};
use bc_jobs::ports::PortError;
use br_core_events::EventMetadata;
use chrono::{DateTime, Utc};
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use crate::db::hydrate::unavailable;
use crate::db::notify;

pub async fn apply_job_events(
    tx: &mut PgConnection,
    job_id: JobId,
    events: &[(EventId, JobEvent)],
    metadata: &EventMetadata,
    occurred_at: DateTime<Utc>,
) -> Result<(), PortError> {
    for (event_id, event) in events {
        write_job_event(tx, event, occurred_at).await?;
        let version = next_version(tx, JOB_AGGREGATE_TYPE, job_id.as_uuid()).await?;
        append(
            tx,
            *event_id,
            job_id.as_uuid(),
            JOB_AGGREGATE_TYPE,
            version,
            event.event_type(),
            event.payload()?,
            metadata,
            occurred_at,
        )
        .await?;
        notify::job_event(tx, *event_id, event).await?;
    }
    Ok(())
}

pub async fn apply_fleet_events(
    tx: &mut PgConnection,
    runner_type_id: RunnerTypeId,
    events: &[(EventId, FleetEvent)],
    metadata: &EventMetadata,
    occurred_at: DateTime<Utc>,
) -> Result<(), PortError> {
    for (event_id, event) in events {
        fleet::write(tx, event, occurred_at).await?;
        let version =
            next_version(tx, RUNNER_TYPE_AGGREGATE_TYPE, runner_type_id.as_uuid()).await?;
        append(
            tx,
            *event_id,
            runner_type_id.as_uuid(),
            RUNNER_TYPE_AGGREGATE_TYPE,
            version,
            event.event_type(),
            event.payload()?,
            metadata,
            occurred_at,
        )
        .await?;
        notify::fleet_event(tx, *event_id, event).await?;
    }
    Ok(())
}

async fn write_job_event(
    tx: &mut PgConnection,
    event: &JobEvent,
    at: DateTime<Utc>,
) -> Result<(), PortError> {
    match event {
        JobEvent::JobQueued(fact) => job::queued(tx, fact, at).await,
        JobEvent::RunDispatched(fact) => run::dispatched(tx, fact, at).await,
        JobEvent::RunStarted(fact) => run::started(tx, fact, at).await,
        JobEvent::RunPlanDeclared(fact) => run::plan_declared(tx, fact, at).await,
        JobEvent::RunStepStarted(fact) => run::step_started(tx, fact).await,
        JobEvent::RunCompleted(fact) => run::completed(tx, fact, at).await,
        JobEvent::RunFailed(fact) => run::failed(tx, fact, at).await,
        JobEvent::RunCancellationRequested(fact) => run::cancellation_requested(tx, fact, at).await,
        JobEvent::RunCancelled(fact) => run::cancelled(tx, fact, at).await,
        JobEvent::RetryScheduled(fact) => run::retry_scheduled(tx, fact).await,
        JobEvent::JobCompleted(fact) => job::completed(tx, fact, at).await,
        JobEvent::JobFailed(fact) => job::failed(tx, fact, at).await,
        JobEvent::JobCancelled(fact) => job::cancelled(tx, fact, at).await,
        JobEvent::ManualRetryStarted(fact) => job::manual_retry_started(tx, fact, at).await,
        JobEvent::JobDeleted(fact) => job::deleted(tx, fact, at).await,
        JobEvent::JobAffordancesChanged(_) => Ok(()),
    }
}

async fn next_version(
    tx: &mut PgConnection,
    aggregate_type: &str,
    aggregate_id: Uuid,
) -> Result<i64, PortError> {
    let row = sqlx::query(
        "SELECT coalesce(max(aggregate_version), 0) + 1 AS version FROM domain_events \
         WHERE aggregate_type = $1 AND aggregate_id = $2",
    )
    .bind(aggregate_type)
    .bind(aggregate_id)
    .fetch_one(&mut *tx)
    .await
    .map_err(unavailable)?;
    Ok(row.get("version"))
}

// eight parameters = the eight columns of the shared DomainEvent envelope, written in one statement
#[allow(clippy::too_many_arguments)]
async fn append(
    tx: &mut PgConnection,
    event_id: EventId,
    aggregate_id: Uuid,
    aggregate_type: &str,
    version: i64,
    event_type: &str,
    payload: serde_json::Value,
    metadata: &EventMetadata,
    occurred_at: DateTime<Utc>,
) -> Result<(), PortError> {
    let metadata = serde_json::to_value(metadata).map_err(|error| PortError::Unavailable {
        detail: error.to_string(),
    })?;
    sqlx::query(
        "INSERT INTO domain_events (id, aggregate_id, aggregate_type, aggregate_version, \
         event_type, payload, metadata, occurred_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
    )
    .bind(event_id.as_uuid())
    .bind(aggregate_id)
    .bind(aggregate_type)
    .bind(version)
    .bind(event_type)
    .bind(payload)
    .bind(metadata)
    .bind(occurred_at)
    .execute(&mut *tx)
    .await
    .map_err(unavailable)?;
    Ok(())
}
