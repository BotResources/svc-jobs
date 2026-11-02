use async_trait::async_trait;
use bc_jobs::domain::ids::{JobId, RunId};
use bc_jobs::domain::mail::inbox::{ItemKind, NoticeType};
use bc_jobs::ports::PortError;
use bc_jobs::ports::restart::RestartWakes;
use chrono::{DateTime, Utc};
use sqlx::PgConnection;
use uuid::Uuid;

use bc_jobs::domain::directory::restart::decide_restart_wake;
use sqlx::Row;

use super::PgStore;
use super::directory_entry::{ENTRY_TABLES, entry_columns, entry_of};
use super::hydrate::unavailable;

fn waking_notice() -> String {
    let types: Vec<String> = NoticeType::WAKING
        .iter()
        .map(|notice_type| format!("'{}'", notice_type.as_wire_str()))
        .collect();
    format!(
        "(i.kind = 'NOTICE' AND i.notice ->> 'type' IN ({}))",
        types.join(", ")
    )
}

fn waking_item_kinds() -> String {
    let kinds: Vec<String> = ItemKind::WAKING
        .iter()
        .map(|kind| format!("i.kind = '{}'", kind.as_db_str()))
        .collect();
    kinds.join(" OR ")
}

fn unread_waiting() -> String {
    let notice = waking_notice();
    let kinds = waking_item_kinds();
    format!(
        "(EXISTS (SELECT 1 FROM inbox_items i WHERE i.address = d.agent_id \
                 AND i.read_at IS NULL \
                 AND (i.kind = 'MAIL' OR {notice} OR {kinds})) \
          OR EXISTS (SELECT 1 FROM inbox_items i WHERE i.address IS NULL \
                 AND i.run_id = d.run_id AND i.read_at IS NULL AND {notice}))"
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

#[async_trait]
impl RestartWakes for PgStore {
    async fn due(&self) -> Result<Vec<RunId>, PortError> {
        let runs: Vec<Uuid> = sqlx::query_scalar(
            "SELECT run_id::uuid FROM restart_wakes ORDER BY requested_at, run_id",
        )
        .fetch_all(self.pool())
        .await
        .map_err(unavailable)?;
        runs.into_iter()
            .map(|run| RunId::new(run).map_err(PortError::from))
            .collect()
    }

    async fn drop_wake(&self, origin_run: RunId) -> Result<(), PortError> {
        let mut connection = self.pool().acquire().await.map_err(unavailable)?;
        consume_wake(&mut connection, origin_run.as_uuid()).await
    }

    async fn origin_job_of(&self, origin_run: RunId) -> Result<Option<JobId>, PortError> {
        self.job_of_run(origin_run.as_uuid()).await
    }
}
