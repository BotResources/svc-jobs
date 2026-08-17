use async_trait::async_trait;
use bc_jobs::domain::fleet::capacity::Capacity;
use bc_jobs::domain::fleet::instance::{RunnerInstance, RunnerInstanceState};
use bc_jobs::domain::fleet::status::ReportedStatus;
use bc_jobs::domain::fleet::{RunnerType, RunnerTypeState};
use bc_jobs::domain::ids::{PresenceSessionId, RunnerTypeId};
use bc_jobs::domain::keys::{InstanceKey, RunnerTypeKey, RunnerVersion};
use bc_jobs::ports::PortError;
use bc_jobs::ports::fleet::{FleetReader, OpenPresenceSession};
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

const OPEN_SESSIONS_SQL: &str = "SELECT rt.type_key, ri.instance_key, s.id::uuid AS session_id \
     FROM runner_presence_sessions s \
     JOIN runner_instances ri ON ri.id = s.instance_id \
     JOIN runner_types rt ON rt.id = ri.runner_type_id \
     WHERE s.disconnected_at IS NULL";

const INSTANCES_SQL: &str = "SELECT ri.runner_type_id::uuid AS runner_type_id, ri.instance_key, \
     s.id::uuid AS session_id, s.version, s.connected_at, s.last_observed_at, \
     c.reported_status AS reported_status, c.capacity AS capacity, \
     c.change_number AS change_number \
     FROM runner_instances ri \
     JOIN runner_types rt ON rt.id = ri.runner_type_id \
     JOIN runner_presence_sessions s ON s.instance_id = ri.id AND s.disconnected_at IS NULL \
     LEFT JOIN LATERAL (SELECT reported_status, capacity, change_number \
        FROM runner_status_changes \
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

    pub async fn lock_runner_type(
        tx: &mut PgConnection,
        runner_type_id: RunnerTypeId,
        key: &RunnerTypeKey,
    ) -> Result<Option<RunnerType>, PortError> {
        let locked: Option<Uuid> =
            sqlx::query_scalar("SELECT id::uuid FROM runner_types WHERE id = $1 FOR UPDATE")
                .bind(runner_type_id.as_uuid())
                .fetch_optional(&mut *tx)
                .await
                .map_err(unavailable)?;
        if locked.is_none() {
            return Err(PortError::ConcurrentModification);
        }
        load_one(&mut *tx, key).await
    }
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
        let declared: Option<i32> = row.get("capacity");
        let declared = declared.ok_or(bc_jobs::JobsError::CorruptState {
            reason_code: "presence_session_without_capacity",
        })?;
        let live = RunnerInstance::hydrate(RunnerInstanceState {
            key: InstanceKey::new(row.get::<String, _>("instance_key"))?,
            session_id: PresenceSessionId::new(row.get("session_id"))?,
            version: RunnerVersion::new(row.get::<String, _>("version"))?,
            reported_status: ReportedStatus::new(reported)?,
            capacity: Capacity::new(u32::try_from(declared).unwrap_or_default())?,
            connected_at: row.get("connected_at"),
            last_observed_at: row.get("last_observed_at"),
            status_change_number: u32::try_from(
                row.get::<Option<i32>, _>("change_number").unwrap_or(1),
            )
            .unwrap_or(1),
        })?;
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

    async fn open_presence_sessions(&self) -> Result<Vec<OpenPresenceSession>, PortError> {
        let rows = sqlx::query(OPEN_SESSIONS_SQL)
            .fetch_all(self.pool())
            .await
            .map_err(unavailable)?;
        rows.iter()
            .map(|row| {
                Ok(OpenPresenceSession {
                    runner_type: RunnerTypeKey::new(row.get::<String, _>("type_key"))?,
                    instance_key: InstanceKey::new(row.get::<String, _>("instance_key"))?,
                    session_id: PresenceSessionId::new(row.get("session_id"))?,
                })
            })
            .collect()
    }
}
