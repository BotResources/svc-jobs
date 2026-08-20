use bc_jobs::domain::fleet::capacity::Capacity;
use bc_jobs::domain::ids::PresenceSessionId;
use bc_jobs::event::fleet::FleetEvent;
use bc_jobs::ports::PortError;
use chrono::{DateTime, Utc};
use sqlx::PgConnection;

use super::refs;
use crate::db::hydrate::unavailable;

pub async fn write(
    tx: &mut PgConnection,
    event: &FleetEvent,
    at: DateTime<Utc>,
) -> Result<(), PortError> {
    match event {
        FleetEvent::RunnerTypeRegistered(fact) => {
            sqlx::query(
                "INSERT INTO registered_runner_types (runner_type_id, lifecycle, registered_at) \
                 VALUES ($1, 'ACTIVE', $2) ON CONFLICT (runner_type_id) DO NOTHING",
            )
            .bind(fact.runner_type_id.as_uuid())
            .bind(at)
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
        }
        FleetEvent::RunnerTypeDeprecated(fact) => {
            set_lifecycle(tx, fact.runner_type_id, "DEPRECATED").await?;
        }
        FleetEvent::RunnerTypeReactivated(fact) => {
            set_lifecycle(tx, fact.runner_type_id, "ACTIVE").await?;
        }
        FleetEvent::RunnerTypeRetired(fact) => {
            set_lifecycle(tx, fact.runner_type_id, "RETIRED").await?;
        }
        FleetEvent::RunnerTypeAffordancesChanged(_) => {}
        FleetEvent::InstanceConnected(fact) => {
            let instance =
                refs::runner_instance_id(tx, fact.runner_type_id.as_uuid(), &fact.instance_key)
                    .await?;
            sqlx::query(
                "INSERT INTO runner_presence_sessions \
                 (id, instance_id, version, connected_at, last_observed_at) \
                 VALUES ($1, $2, $3, $4, $4)",
            )
            .bind(fact.session_id.as_uuid())
            .bind(instance)
            .bind(fact.version.as_str())
            .bind(at)
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
            status_change(
                tx,
                fact.session_id,
                1,
                fact.reported_status.as_str(),
                fact.capacity,
                at,
            )
            .await?;
        }
        FleetEvent::InstanceStatusReported(fact) => {
            sqlx::query(
                "UPDATE runner_presence_sessions SET version = $2, \
                 last_observed_at = greatest(last_observed_at, $3) WHERE id = $1",
            )
            .bind(fact.session_id.as_uuid())
            .bind(fact.version.as_str())
            .bind(at)
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
            status_change(
                tx,
                fact.session_id,
                i32::try_from(fact.change_number).unwrap_or(i32::MAX),
                fact.reported_status.as_str(),
                fact.capacity,
                at,
            )
            .await?;
        }
        FleetEvent::InstanceDisconnected(fact) => {
            sqlx::query(
                "UPDATE runner_presence_sessions \
                 SET disconnected_at = greatest(connected_at, $2), \
                 disconnect_reason_code = $3 \
                 WHERE id = $1 AND disconnected_at IS NULL",
            )
            .bind(fact.session_id.as_uuid())
            .bind(at)
            .bind(fact.reason_code.as_str())
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
        }
    }
    Ok(())
}

async fn set_lifecycle(
    tx: &mut PgConnection,
    runner_type_id: bc_jobs::domain::ids::RunnerTypeId,
    lifecycle: &str,
) -> Result<(), PortError> {
    sqlx::query("UPDATE registered_runner_types SET lifecycle = $2 WHERE runner_type_id = $1")
        .bind(runner_type_id.as_uuid())
        .bind(lifecycle)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
    Ok(())
}

async fn status_change(
    tx: &mut PgConnection,
    session: PresenceSessionId,
    change_number: i32,
    status: &str,
    capacity: Capacity,
    at: DateTime<Utc>,
) -> Result<(), PortError> {
    sqlx::query(
        "INSERT INTO runner_status_changes (session_id, change_number, reported_status, \
         capacity, observed_at) VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(session.as_uuid())
    .bind(change_number)
    .bind(status)
    .bind(capacity.get_i32())
    .bind(at)
    .execute(&mut *tx)
    .await
    .map_err(unavailable)?;
    Ok(())
}
