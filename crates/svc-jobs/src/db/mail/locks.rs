use bc_jobs::ports::PortError;
use sqlx::PgConnection;
use uuid::Uuid;

use crate::db::hydrate::unavailable;

fn key(mail_id: Uuid) -> String {
    format!("mail_attachments:{mail_id}")
}

pub async fn lock_mail_attachments(tx: &mut PgConnection, mail_id: Uuid) -> Result<(), PortError> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(key(mail_id))
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
    Ok(())
}

pub async fn try_lock_mail_attachments(
    tx: &mut PgConnection,
    mail_id: Uuid,
) -> Result<bool, PortError> {
    sqlx::query_scalar("SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(key(mail_id))
        .fetch_one(&mut *tx)
        .await
        .map_err(unavailable)
}
