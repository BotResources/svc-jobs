use bc_jobs::domain::keys::{DisplayName, InstanceKey, ProducerKey, RunnerTypeKey};
use bc_jobs::domain::references::KnownUser;
use bc_jobs::ports::PortError;
use chrono::{DateTime, Utc};
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use crate::db::hydrate::unavailable;

pub async fn runner_type_id(tx: &mut PgConnection, key: &RunnerTypeKey) -> Result<Uuid, PortError> {
    upsert_key(
        tx,
        "INSERT INTO runner_types (id, type_key) VALUES ($1, $2) \
         ON CONFLICT (type_key) DO UPDATE SET type_key = EXCLUDED.type_key \
         RETURNING id::uuid AS id",
        key.as_str(),
    )
    .await
}

pub async fn producer_id(tx: &mut PgConnection, key: &ProducerKey) -> Result<Uuid, PortError> {
    upsert_key(
        tx,
        "INSERT INTO producers (id, bc_key) VALUES ($1, $2) \
         ON CONFLICT (bc_key) DO UPDATE SET bc_key = EXCLUDED.bc_key \
         RETURNING id::uuid AS id",
        key.as_str(),
    )
    .await
}

async fn upsert_key(tx: &mut PgConnection, sql: &str, key: &str) -> Result<Uuid, PortError> {
    let row = sqlx::query(sql)
        .bind(Uuid::now_v7())
        .bind(key)
        .fetch_one(&mut *tx)
        .await
        .map_err(unavailable)?;
    Ok(row.get("id"))
}

pub async fn source_entity_id(
    tx: &mut PgConnection,
    producer: Uuid,
    external_id: Uuid,
) -> Result<Uuid, PortError> {
    let row = sqlx::query(
        "INSERT INTO source_entities (id, producer_id, external_id) VALUES ($1, $2, $3) \
         ON CONFLICT (producer_id, external_id) \
         DO UPDATE SET external_id = EXCLUDED.external_id \
         RETURNING id::uuid AS id",
    )
    .bind(Uuid::now_v7())
    .bind(producer)
    .bind(external_id)
    .fetch_one(&mut *tx)
    .await
    .map_err(unavailable)?;
    Ok(row.get("id"))
}

pub async fn lock_source_entity(
    tx: &mut PgConnection,
    producer: &ProducerKey,
    external_id: Uuid,
) -> Result<(), PortError> {
    let producer_id = producer_id(tx, producer).await?;
    let entity_id = source_entity_id(tx, producer_id, external_id).await?;
    sqlx::query("SELECT id FROM source_entities WHERE id = $1 FOR UPDATE")
        .bind(entity_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?;
    Ok(())
}

pub async fn runner_instance_id(
    tx: &mut PgConnection,
    runner_type: Uuid,
    instance_key: &InstanceKey,
) -> Result<Uuid, PortError> {
    let row = sqlx::query(
        "INSERT INTO runner_instances (id, runner_type_id, instance_key) VALUES ($1, $2, $3) \
         ON CONFLICT (runner_type_id, instance_key) \
         DO UPDATE SET instance_key = EXCLUDED.instance_key \
         RETURNING id::uuid AS id",
    )
    .bind(Uuid::now_v7())
    .bind(runner_type)
    .bind(instance_key.as_str())
    .fetch_one(&mut *tx)
    .await
    .map_err(unavailable)?;
    Ok(row.get("id"))
}

pub async fn observe_known_user(
    tx: &mut PgConnection,
    user: &KnownUser,
    at: DateTime<Utc>,
) -> Result<Uuid, PortError> {
    observe_user(tx, user.id().0, user.display_name(), at).await
}

pub async fn observe_user(
    tx: &mut PgConnection,
    id: Uuid,
    display_name: &DisplayName,
    at: DateTime<Utc>,
) -> Result<Uuid, PortError> {
    sqlx::query(
        "INSERT INTO known_users (id, display_name, observed_at) VALUES ($1, $2, $3) \
         ON CONFLICT (id) DO UPDATE SET display_name = EXCLUDED.display_name, \
         observed_at = EXCLUDED.observed_at",
    )
    .bind(id)
    .bind(display_name.as_str())
    .bind(at)
    .execute(&mut *tx)
    .await
    .map_err(unavailable)?;
    Ok(id)
}
