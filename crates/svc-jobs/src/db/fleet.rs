use async_trait::async_trait;
use bc_jobs::domain::fleet::instance::RunnerInstance;
use bc_jobs::domain::fleet::{RunnerType, RunnerTypeState};
use bc_jobs::domain::ids::{PresenceSessionId, RunnerTypeId};
use bc_jobs::domain::keys::{InstanceKey, ReportedStatus, RunnerTypeKey, RunnerVersion};
use bc_jobs::event::fleet::FleetEvent;
use bc_jobs::ports::PortError;
use bc_jobs::ports::fleet::FleetReader;
use chrono::{DateTime, Utc};
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use super::PgStore;
use super::apply::refs;
use super::hydrate::unavailable;

const TYPES_SQL: &str = "SELECT rt.id::uuid AS id, rt.type_key, \
     min(s.connected_at) AS registered_at \
     FROM runner_types rt \
     JOIN runner_instances ri ON ri.runner_type_id = rt.id \
     JOIN runner_presence_sessions s ON s.instance_id = ri.id \
     WHERE $1::text IS NULL OR rt.type_key = $1 \
     GROUP BY rt.id, rt.type_key";

const INSTANCES_SQL: &str = "SELECT ri.runner_type_id::uuid AS runner_type_id, ri.instance_key, \
     s.id::uuid AS session_id, s.version, s.connected_at, s.last_observed_at, \
     c.reported_status AS reported_status, c.change_number AS change_number \
     FROM runner_instances ri \
     JOIN runner_types rt ON rt.id = ri.runner_type_id \
     JOIN runner_presence_sessions s ON s.instance_id = ri.id AND s.disconnected_at IS NULL \
     LEFT JOIN LATERAL (SELECT reported_status, change_number FROM runner_status_changes \
        WHERE session_id = s.id ORDER BY change_number DESC LIMIT 1) c ON true \
     WHERE $1::text IS NULL OR rt.type_key = $1";

impl PgStore {
    pub async fn runner_type_keys_with_jobs(&self) -> Result<Vec<RunnerTypeKey>, PortError> {
        let rows = sqlx::query(
            "SELECT DISTINCT rt.type_key FROM runner_types rt JOIN jobs j \
             ON j.runner_type_id = rt.id",
        )
        .fetch_all(self.pool())
        .await
        .map_err(unavailable)?;
        rows.iter()
            .map(|row| {
                RunnerTypeKey::new(row.get::<String, _>("type_key")).map_err(PortError::from)
            })
            .collect()
    }

    pub async fn ensure_runner_type(&self, key: &RunnerTypeKey) -> Result<Uuid, PortError> {
        let mut tx = self.begin().await?;
        let id = refs::runner_type_id(&mut tx, key).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(id)
    }

    pub async fn load_fleet_in(
        tx: &mut PgConnection,
        key: &RunnerTypeKey,
    ) -> Result<Option<RunnerType>, PortError> {
        load_one(&mut *tx, key).await
    }

    pub async fn apply_fleet_event(
        tx: &mut PgConnection,
        event: &FleetEvent,
        at: DateTime<Utc>,
    ) -> Result<(), PortError> {
        match event {
            FleetEvent::RunnerTypeRegistered(fact) => {
                sqlx::query(
                    "INSERT INTO runner_types (id, type_key) VALUES ($1, $2) \
                     ON CONFLICT (type_key) DO NOTHING",
                )
                .bind(fact.runner_type_id.as_uuid())
                .bind(fact.runner_type.as_str())
                .execute(&mut *tx)
                .await
                .map_err(unavailable)?;
            }
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
                status_change(tx, fact.session_id, 1, fact.reported_status.as_str(), at).await?;
            }
            FleetEvent::InstanceStatusReported(fact) => {
                sqlx::query(
                    "UPDATE runner_presence_sessions SET version = $2, last_observed_at = $3 \
                     WHERE id = $1",
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
                    at,
                )
                .await?;
            }
            FleetEvent::InstanceDisconnected(fact) => {
                sqlx::query(
                    "UPDATE runner_presence_sessions \
                     SET disconnected_at = $2, disconnect_reason_code = $3 \
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
}

async fn status_change(
    tx: &mut PgConnection,
    session: PresenceSessionId,
    change_number: i32,
    status: &str,
    at: DateTime<Utc>,
) -> Result<(), PortError> {
    sqlx::query(
        "INSERT INTO runner_status_changes (session_id, change_number, reported_status, \
         observed_at) VALUES ($1, $2, $3, $4) ON CONFLICT (session_id, change_number) DO NOTHING",
    )
    .bind(session.as_uuid())
    .bind(change_number)
    .bind(status)
    .bind(at)
    .execute(&mut *tx)
    .await
    .map_err(unavailable)?;
    Ok(())
}

async fn load_types(
    executor: &mut PgConnection,
    only: Option<&RunnerTypeKey>,
) -> Result<Vec<RunnerType>, PortError> {
    let only = only.map(RunnerTypeKey::as_str);
    let mut instances: std::collections::HashMap<Uuid, Vec<RunnerInstance>> =
        std::collections::HashMap::new();
    for row in sqlx::query(INSTANCES_SQL)
        .bind(only)
        .fetch_all(&mut *executor)
        .await
        .map_err(unavailable)?
    {
        let reported: Option<String> = row.get("reported_status");
        let reported = reported.ok_or(bc_jobs::JobsError::CorruptState {
            reason_code: "presence_session_without_status",
        })?;
        let live = RunnerInstance::hydrate(
            InstanceKey::new(row.get::<String, _>("instance_key"))?,
            PresenceSessionId::new(row.get("session_id"))?,
            RunnerVersion::new(row.get::<String, _>("version"))?,
            ReportedStatus::new(reported)?,
            row.get("connected_at"),
            row.get("last_observed_at"),
            u32::try_from(row.get::<Option<i32>, _>("change_number").unwrap_or(1)).unwrap_or(1),
        )?;
        instances
            .entry(row.get("runner_type_id"))
            .or_default()
            .push(live);
    }
    let mut types = Vec::new();
    for row in sqlx::query(TYPES_SQL)
        .bind(only)
        .fetch_all(&mut *executor)
        .await
        .map_err(unavailable)?
    {
        let id: Uuid = row.get("id");
        types.push(RunnerType::hydrate(RunnerTypeState {
            id: RunnerTypeId::new(id)?,
            key: RunnerTypeKey::new(row.get::<String, _>("type_key"))?,
            registered_at: row.get::<DateTime<Utc>, _>("registered_at"),
            instances: instances.remove(&id).unwrap_or_default(),
        })?);
    }
    Ok(types)
}

async fn load_one(
    executor: &mut PgConnection,
    key: &RunnerTypeKey,
) -> Result<Option<RunnerType>, PortError> {
    Ok(load_types(executor, Some(key)).await?.pop())
}

#[async_trait]
impl FleetReader for PgStore {
    async fn load(&self, key: &RunnerTypeKey) -> Result<Option<RunnerType>, PortError> {
        let mut connection = self.pool().acquire().await.map_err(unavailable)?;
        load_one(&mut connection, key).await
    }

    async fn load_all(&self) -> Result<Vec<RunnerType>, PortError> {
        let mut connection = self.pool().acquire().await.map_err(unavailable)?;
        load_types(&mut connection, None).await
    }
}
