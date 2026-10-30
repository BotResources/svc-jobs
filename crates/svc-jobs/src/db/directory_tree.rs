use async_trait::async_trait;
use bc_jobs::domain::ids::JobId;
use bc_jobs::ports::PortError;
use bc_jobs::ports::directory::{ListedEntry, TreeDirectory};
use sqlx::PgConnection;

use super::PgStore;
use super::directory_entry::{ENTRY_TABLES, entry_columns, entry_of};
use super::hydrate::unavailable;

#[async_trait]
impl TreeDirectory for PgStore {
    async fn entries_of_tree(&self, job_id: JobId) -> Result<Option<Vec<ListedEntry>>, PortError> {
        let mut connection = self.pool().acquire().await.map_err(unavailable)?;
        let Some(tree) = PgStore::initial_job_of(&mut connection, job_id).await? else {
            return Ok(None);
        };
        entries_in(&mut connection, tree).await.map(Some)
    }
}

pub async fn entries_in(
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
