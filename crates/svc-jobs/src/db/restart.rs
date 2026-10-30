use bc_jobs::ports::PortError;
use chrono::{DateTime, Utc};
use sqlx::PgConnection;
use uuid::Uuid;

use bc_jobs::domain::directory::restart::decide_restart_wake;
use sqlx::Row;

use super::PgStore;
use super::directory_entry::{ENTRY_TABLES, entry_columns, entry_of};
use super::hydrate::unavailable;

const FAILURE_NOTICE: &str = "i.kind = 'NOTICE' AND i.notice ->> 'type' = 'JOB_FAILED'";

fn unread_waiting() -> String {
    format!(
        "(EXISTS (SELECT 1 FROM inbox_items i WHERE i.address = d.agent_id \
                 AND i.read_at IS NULL AND (i.kind = 'MAIL' OR ({FAILURE_NOTICE}))) \
          OR EXISTS (SELECT 1 FROM inbox_items i WHERE i.address IS NULL \
                 AND i.run_id = d.run_id AND i.read_at IS NULL AND {FAILURE_NOTICE}))"
    )
}

async fn runs_due_for_restart(
    tx: &mut PgConnection,
    addresses: &[Uuid],
    run_id: Option<Uuid>,
) -> Result<Vec<Uuid>, PortError> {
    let sql = format!(
        "SELECT {}, {} AS unread_waiting {} WHERE d.agent_id = ANY($1) OR d.run_id = $2",
        entry_columns(),
        unread_waiting(),
        ENTRY_TABLES
    );
    let rows = sqlx::query(&sql)
        .bind(addresses)
        .bind(run_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(unavailable)?;
    let mut due = Vec::new();
    for row in &rows {
        let (entry, obstacles) = entry_of(row)?;
        if decide_restart_wake(&entry, obstacles, row.get("unread_waiting")).is_due() {
            due.push(entry.run_id.as_uuid());
        }
    }
    due.sort();
    due.dedup();
    Ok(due)
}

async fn enqueue(tx: &mut PgConnection, runs: &[Uuid], at: DateTime<Utc>) -> Result<(), PortError> {
    sqlx::query(
        "INSERT INTO restart_wakes (run_id, requested_at) \
         SELECT run_id, $2 FROM unnest($1::uuid[]) AS run_id ON CONFLICT (run_id) DO NOTHING",
    )
    .bind(runs)
    .bind(at)
    .execute(&mut *tx)
    .await
    .map_err(unavailable)?;
    Ok(())
}

pub async fn wake_addresses(
    tx: &mut PgConnection,
    addresses: &[Uuid],
    at: DateTime<Utc>,
) -> Result<(), PortError> {
    let runs = runs_due_for_restart(tx, addresses, None).await?;
    enqueue(tx, &runs, at).await
}

pub async fn wake_run(
    tx: &mut PgConnection,
    run_id: Uuid,
    at: DateTime<Utc>,
) -> Result<(), PortError> {
    let runs = runs_due_for_restart(tx, &[], Some(run_id)).await?;
    enqueue(tx, &runs, at).await
}

pub async fn restart_still_due(tx: &mut PgConnection, origin_run: Uuid) -> Result<bool, PortError> {
    Ok(runs_due_for_restart(tx, &[], Some(origin_run))
        .await?
        .contains(&origin_run))
}

pub async fn consume_wake(tx: &mut PgConnection, origin_run: Uuid) -> Result<(), PortError> {
    sqlx::query("DELETE FROM restart_wakes WHERE run_id = $1")
        .bind(origin_run)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
    Ok(())
}

impl PgStore {
    pub async fn due_wakes(&self) -> Result<Vec<Uuid>, PortError> {
        sqlx::query_scalar("SELECT run_id::uuid FROM restart_wakes ORDER BY requested_at, run_id")
            .fetch_all(self.pool())
            .await
            .map_err(unavailable)
    }

    pub async fn drop_wake(&self, origin_run: Uuid) -> Result<(), PortError> {
        let mut connection = self.pool().acquire().await.map_err(unavailable)?;
        consume_wake(&mut connection, origin_run).await
    }
}
