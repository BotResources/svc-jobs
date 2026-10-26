use bc_jobs::event::user::UserEvent;
use bc_jobs::ports::PortError;
use chrono::{DateTime, Utc};
use sqlx::PgConnection;

use super::refs;
use crate::db::hydrate::unavailable;

pub async fn write(
    tx: &mut PgConnection,
    event: &UserEvent,
    at: DateTime<Utc>,
) -> Result<(), PortError> {
    match event {
        UserEvent::MaxPrioritySet(fact) => {
            let set_by = refs::observe_known_user(tx, &fact.set_by, at).await?;
            sqlx::query(
                "INSERT INTO user_max_priorities (user_id, max_priority, set_by_id, set_at) \
                 VALUES ($1, $2, $3, $4) \
                 ON CONFLICT (user_id) DO UPDATE SET max_priority = EXCLUDED.max_priority, \
                 set_by_id = EXCLUDED.set_by_id, set_at = EXCLUDED.set_at",
            )
            .bind(fact.user_id.as_uuid())
            .bind(fact.max_priority.get_i16())
            .bind(set_by)
            .bind(at)
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
        }
        UserEvent::MaxPriorityCleared(fact) => {
            refs::observe_known_user(tx, &fact.cleared_by, at).await?;
            sqlx::query("DELETE FROM user_max_priorities WHERE user_id = $1")
                .bind(fact.user_id.as_uuid())
                .execute(&mut *tx)
                .await
                .map_err(unavailable)?;
        }
    }
    Ok(())
}
