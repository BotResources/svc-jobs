use bc_jobs::domain::fleet::capacity::Capacity;
use bc_jobs::domain::fleet::instance::{RunnerInstance, RunnerInstanceState};
use bc_jobs::domain::fleet::lifecycle::RunnerTypeLifecycle;
use bc_jobs::domain::fleet::status::ReportedStatus;
use bc_jobs::domain::fleet::{RunnerType, RunnerTypeState};
use bc_jobs::domain::ids::{PresenceSessionId, RunnerTypeId};
use bc_jobs::domain::keys::{InstanceKey, RunnerTypeKey, RunnerVersion};
use bc_jobs::error::JobsError;
use bc_jobs::ports::PortError;
use chrono::{DateTime, Utc};
use sqlx::postgres::PgRow;
use sqlx::{PgConnection, Row};
use std::collections::HashMap;
use uuid::Uuid;

use crate::db::PgStore;
use crate::db::hydrate::unavailable;

const TYPES_SQL: &str = "SELECT rt.id::uuid AS id, rt.type_key, \
     registered.lifecycle::text AS lifecycle, registered.registered_at \
     FROM runner_types rt \
     JOIN registered_runner_types registered ON registered.runner_type_id = rt.id \
     WHERE $1::text IS NULL OR rt.type_key = $1 \
     ORDER BY rt.type_key";

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CorruptionPolicy {
    FailTheLoad,
    IsolateTheRunnerType,
}

impl PgStore {
    pub async fn runner_type_route_id(
        &self,
        key: &RunnerTypeKey,
    ) -> Result<Option<Uuid>, PortError> {
        let mut connection = self.pool().acquire().await.map_err(unavailable)?;
        route_id(&mut connection, key).await
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
        let actual = upsert_route(&mut *tx, runner_type_id.as_uuid(), key).await?;
        if actual != runner_type_id.as_uuid() {
            return Err(PortError::ConcurrentModification);
        }
        load_one(&mut *tx, key).await
    }
}

pub async fn route_id(
    tx: &mut PgConnection,
    key: &RunnerTypeKey,
) -> Result<Option<Uuid>, PortError> {
    sqlx::query_scalar("SELECT id::uuid FROM runner_types WHERE type_key = $1")
        .bind(key.as_str())
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)
}

pub async fn upsert_route(
    tx: &mut PgConnection,
    proposed_id: Uuid,
    key: &RunnerTypeKey,
) -> Result<Uuid, PortError> {
    sqlx::query_scalar(
        "INSERT INTO runner_types (id, type_key) VALUES ($1, $2) \
         ON CONFLICT (type_key) DO UPDATE SET type_key = EXCLUDED.type_key \
         RETURNING id::uuid",
    )
    .bind(proposed_id)
    .bind(key.as_str())
    .fetch_one(&mut *tx)
    .await
    .map_err(unavailable)
}

pub async fn registered_lifecycle(
    tx: &mut PgConnection,
    runner_type_id: Uuid,
) -> Result<Option<RunnerTypeLifecycle>, PortError> {
    let stored: Option<String> = sqlx::query_scalar(
        "SELECT lifecycle::text FROM registered_runner_types WHERE runner_type_id = $1",
    )
    .bind(runner_type_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(unavailable)?;
    stored
        .map(|stored| RunnerTypeLifecycle::from_db_str(&stored))
        .transpose()
        .map_err(PortError::from)
}

pub async fn load_types(
    executor: &mut PgConnection,
    only: Option<&RunnerTypeKey>,
    policy: CorruptionPolicy,
) -> Result<Vec<RunnerType>, PortError> {
    let only = only.map(RunnerTypeKey::as_str);
    let mut loaded = load_instances(&mut *executor, only).await?;
    let mut types = Vec::new();
    for row in sqlx::query(TYPES_SQL)
        .bind(only)
        .fetch_all(&mut *executor)
        .await
        .map_err(unavailable)?
    {
        let id: Uuid = row.get("id");
        let key = RunnerTypeKey::new(row.get::<String, _>("type_key"))?;
        let hydrated = match loaded.corrupt.remove(&id) {
            Some(corruption) => Err(corruption),
            None => RunnerType::hydrate(RunnerTypeState {
                id: RunnerTypeId::new(id)?,
                key: key.clone(),
                registered_at: row.get::<DateTime<Utc>, _>("registered_at"),
                lifecycle: RunnerTypeLifecycle::from_db_str(&row.get::<String, _>("lifecycle"))?,
                instances: loaded.healthy.remove(&id).unwrap_or_default(),
            }),
        };
        match hydrated {
            Ok(runner_type) => types.push(runner_type),
            Err(corruption) => match policy {
                CorruptionPolicy::FailTheLoad => return Err(corruption.into()),
                CorruptionPolicy::IsolateTheRunnerType => tracing::error!(
                    runner_type = key.as_str(),
                    corruption_code = corruption.code(),
                    corruption_params = %corruption.params(),
                    "stored fleet state this listing cannot load: the runner type is left out of \
                     the listing so the healthy ones still answer, and addressing it directly \
                     still fails"
                ),
            },
        }
    }
    Ok(types)
}

pub async fn load_one(
    executor: &mut PgConnection,
    key: &RunnerTypeKey,
) -> Result<Option<RunnerType>, PortError> {
    Ok(
        load_types(executor, Some(key), CorruptionPolicy::FailTheLoad)
            .await?
            .pop(),
    )
}

struct LoadedInstances {
    healthy: HashMap<Uuid, Vec<RunnerInstance>>,
    corrupt: HashMap<Uuid, JobsError>,
}

async fn load_instances(
    executor: &mut PgConnection,
    only: Option<&str>,
) -> Result<LoadedInstances, PortError> {
    let mut loaded = LoadedInstances {
        healthy: HashMap::new(),
        corrupt: HashMap::new(),
    };
    for row in sqlx::query(INSTANCES_SQL)
        .bind(only)
        .fetch_all(&mut *executor)
        .await
        .map_err(unavailable)?
    {
        let runner_type_id: Uuid = row.get("runner_type_id");
        match instance_of(&row) {
            Ok(live) => loaded.healthy.entry(runner_type_id).or_default().push(live),
            Err(corruption) => {
                loaded.corrupt.entry(runner_type_id).or_insert(corruption);
            }
        }
    }
    Ok(loaded)
}

fn instance_of(row: &PgRow) -> Result<RunnerInstance, JobsError> {
    let reported: Option<String> = row.get("reported_status");
    let reported = reported.ok_or(JobsError::CorruptState {
        reason_code: "presence_session_without_status",
    })?;
    let declared: Option<i32> = row.get("capacity");
    let declared = declared.ok_or(JobsError::CorruptState {
        reason_code: "presence_session_without_capacity",
    })?;
    RunnerInstance::hydrate(RunnerInstanceState {
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
    })
}
