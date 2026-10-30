use bc_jobs::domain::ids::JobId;
use bc_jobs::ports::PortError;
use bc_jobs::ports::directory::ListedEntry;
use sqlx::PgConnection;

use super::directory_entry::{ENTRY_TABLES, entry_columns, entry_of};
use super::hydrate::unavailable;

pub(super) async fn entries_in(
    connection: &mut PgConnection,
    tree: JobId,
) -> Result<Vec<ListedEntry>, PortError> {
    let sql = format!(
        "SELECT {} {} WHERE d.initial_job_id = $1 ORDER BY d.registered_at, d.agent_id",
        entry_columns(),
        ENTRY_TABLES
    );
    let rows = sqlx::query(&sql)
        .bind(tree.as_uuid())
        .fetch_all(&mut *connection)
        .await
        .map_err(unavailable)?;
    rows.iter()
        .map(|row| entry_of(row).map(|(entry, obstacles)| ListedEntry { entry, obstacles }))
        .collect()
}
