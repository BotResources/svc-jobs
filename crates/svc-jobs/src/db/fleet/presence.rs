use bc_jobs::domain::ids::PresenceSessionId;
use bc_jobs::domain::keys::{InstanceKey, RunnerTypeKey};
use bc_jobs::ports::PortError;
use bc_jobs::ports::fleet::{ClosedPresenceSession, OpenPresenceSession};
use sqlx::Row;

use crate::db::PgStore;
use crate::db::hydrate::unavailable;

const OPEN_SESSIONS_SQL: &str = "SELECT rt.type_key, ri.instance_key, s.id::uuid AS session_id \
     FROM runner_presence_sessions s \
     JOIN runner_instances ri ON ri.id = s.instance_id \
     JOIN runner_types rt ON rt.id = ri.runner_type_id \
     WHERE s.disconnected_at IS NULL \
     ORDER BY s.connected_at, s.id LIMIT 200";

const CLOSED_SESSION_SQL: &str = "SELECT connected_at, disconnected_at \
     FROM runner_presence_sessions \
     WHERE id = $1 AND disconnected_at IS NOT NULL";

pub async fn open_sessions(store: &PgStore) -> Result<Vec<OpenPresenceSession>, PortError> {
    let rows = sqlx::query(OPEN_SESSIONS_SQL)
        .fetch_all(store.pool())
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

pub async fn closed_session(
    store: &PgStore,
    session_id: PresenceSessionId,
) -> Result<Option<ClosedPresenceSession>, PortError> {
    let row = sqlx::query(CLOSED_SESSION_SQL)
        .bind(session_id.as_uuid())
        .fetch_optional(store.pool())
        .await
        .map_err(unavailable)?;
    Ok(row.map(|row| ClosedPresenceSession {
        connected_at: row.get("connected_at"),
        disconnected_at: row.get("disconnected_at"),
    }))
}
