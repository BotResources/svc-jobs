use bc_jobs::domain::erasure::{
    ERASED_CONVERSATION_ID, ERASED_TEXT, mail_is_between_removed_addresses,
};
use bc_jobs::ports::PortError;
use chrono::{DateTime, Utc};
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use crate::db::hydrate::unavailable;

pub const CANDIDATE_MAILS_SQL: &str = "\
    SELECT m.mail_id, \
        (SELECT count(*) FROM directory_removed_addresses d \
         WHERE d.agent_id = m.sender_address) > 0 AS sender_removed, \
        ARRAY(SELECT (SELECT count(*) FROM directory_removed_addresses d \
                      WHERE d.agent_id = r.address) > 0 \
              FROM mail_recipients r WHERE r.mail_id = m.mail_id) AS recipients_removed \
    FROM mails m \
    WHERE m.content_erased_at IS NULL \
      AND m.mail_id IN (SELECT mail_id FROM mails WHERE sender_address = ANY($1) \
                        UNION \
                        SELECT mail_id FROM mail_recipients WHERE address = ANY($1)) \
    FOR UPDATE OF m";

pub async fn erase_mail_content_between_removed_addresses(
    tx: &mut PgConnection,
    removed: &[Uuid],
    at: DateTime<Utc>,
) -> Result<(), PortError> {
    if removed.is_empty() {
        return Ok(());
    }
    let candidates = sqlx::query(CANDIDATE_MAILS_SQL)
        .bind(removed)
        .fetch_all(&mut *tx)
        .await
        .map_err(unavailable)?;
    let erasable: Vec<Uuid> = candidates
        .iter()
        .filter(|row| {
            mail_is_between_removed_addresses(
                row.get("sender_removed"),
                &row.get::<Vec<bool>, _>("recipients_removed"),
            )
        })
        .map(|row| row.get("mail_id"))
        .collect();
    if erasable.is_empty() {
        return Ok(());
    }
    sqlx::query(
        "UPDATE mails SET author = $2, source_conversation_id = $3, subject = '', body = '', \
            content_erased_at = $4 \
         WHERE mail_id = ANY($1)",
    )
    .bind(&erasable)
    .bind(ERASED_TEXT)
    .bind(ERASED_CONVERSATION_ID)
    .bind(at)
    .execute(&mut *tx)
    .await
    .map_err(unavailable)?;
    sqlx::query("DELETE FROM mail_attachments WHERE mail_id = ANY($1)")
        .bind(&erasable)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
    Ok(())
}
