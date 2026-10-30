use bc_jobs::domain::ids::{AgentId, ItemId, JobId, ResultId, RunId};
use bc_jobs::domain::job::JobCall;
use bc_jobs::domain::mail::inbox::{ItemKind, Notice};
use bc_jobs::domain::payload::PayloadAttachment;
use bc_jobs::domain::requester::Requester;
use bc_jobs::domain::runner::call_notices::CallNotices;
use bc_jobs::ports::PortError;
use bc_jobs::ports::environment::IdFactory;
use br_core_events::EventMetadata;
use chrono::{DateTime, Utc};
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use super::doorbell::{Arrival, Ring, ring};
use super::hydrate::unavailable;

const POSITION_SQL: &str = "WITH waiting AS ( \
        SELECT w.job_id, w.runner_type_id, w.root_job_id, w.effective_priority, w.created_at, \
            min(w.created_at) OVER (PARTITION BY w.runner_type_id, w.root_job_id) \
                AS oldest_waiting, \
            row_number() OVER (PARTITION BY w.runner_type_id, w.root_job_id, \
                w.effective_priority ORDER BY w.created_at, w.job_id) AS turn \
        FROM waiting_jobs w \
        WHERE w.runner_type_id = (SELECT runner_type_id FROM waiting_jobs WHERE job_id = $1)), \
     ranked AS ( \
        SELECT o.job_id, row_number() OVER ( \
            ORDER BY o.effective_priority DESC, o.turn, turns.last_served_at ASC NULLS FIRST, \
                o.oldest_waiting, o.created_at, o.job_id) AS position \
        FROM waiting o \
        LEFT JOIN root_dispatch_turns turns \
            ON turns.runner_type_id = o.runner_type_id AND turns.root_job_id = o.root_job_id) \
     SELECT position FROM ranked WHERE job_id = $1";

const LIVE_RUNS_REACHED_SQL: &str = "WITH RECURSIVE acting(run_id) AS ( \
        SELECT $1::uuid \
      UNION \
        SELECT r.id FROM acting a \
        JOIN restart_origins ro ON ro.restarts_run_id = a.run_id \
        JOIN runs r ON r.job_id = ro.job_id) \
     SELECT a.run_id::uuid FROM acting a \
     WHERE EXISTS (SELECT 1 FROM run_starts s WHERE s.run_id = a.run_id) \
       AND NOT EXISTS (SELECT 1 FROM run_terminals t WHERE t.run_id = a.run_id)";

pub async fn call_notices_of(
    tx: &mut PgConnection,
    job_id: JobId,
) -> Result<Option<CallNotices>, PortError> {
    let row: Option<(Option<serde_json::Value>, Option<serde_json::Value>)> =
        sqlx::query_as("SELECT requester, call FROM jobs WHERE id = $1")
            .bind(job_id.as_uuid())
            .fetch_optional(&mut *tx)
            .await
            .map_err(unavailable)?;
    let Some((Some(requester), Some(call))) = row else {
        return Ok(None);
    };
    let requester: Requester = serde_json::from_value(requester).map_err(stored)?;
    let call: JobCall = serde_json::from_value(call).map_err(stored)?;
    Ok(CallNotices::of(&requester, Some(&call)))
}

fn stored(error: serde_json::Error) -> PortError {
    PortError::Unavailable {
        detail: error.to_string(),
    }
}

pub async fn queue_position(
    tx: &mut PgConnection,
    job_id: JobId,
) -> Result<Option<u32>, PortError> {
    let found: Option<i64> = sqlx::query_scalar(POSITION_SQL)
        .bind(job_id.as_uuid())
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?;
    Ok(found.and_then(|position| u32::try_from(position).ok()))
}

#[derive(Debug, Clone)]
pub struct KeptResult {
    pub result_id: ResultId,
    pub message: String,
    pub attachments: Vec<PayloadAttachment>,
}

pub async fn result_of_run(
    tx: &mut PgConnection,
    job_id: JobId,
    run_id: RunId,
) -> Result<Option<KeptResult>, PortError> {
    let Some(row) = sqlx::query(
        "SELECT result_id::uuid AS result_id, message, attachments FROM job_results \
         WHERE job_id = $1 AND produced_by_run_id = $2 ORDER BY kept_at DESC LIMIT 1",
    )
    .bind(job_id.as_uuid())
    .bind(run_id.as_uuid())
    .fetch_optional(&mut *tx)
    .await
    .map_err(unavailable)?
    else {
        return Ok(None);
    };
    let attachments =
        serde_json::from_value(row.get("attachments")).map_err(|error| PortError::Unavailable {
            detail: error.to_string(),
        })?;
    Ok(Some(KeptResult {
        result_id: ResultId::new(row.get("result_id"))?,
        message: row.get("message"),
        attachments,
    }))
}

pub struct NoticeFor {
    pub run_id: RunId,
    pub address: Option<AgentId>,
}

pub struct Delivering<'a> {
    pub ids: &'a dyn IdFactory,
    pub metadata: &'a EventMetadata,
    pub at: DateTime<Utc>,
}

pub async fn insert_notice(
    tx: &mut PgConnection,
    target: NoticeFor,
    notice: &Notice,
    delivering: Delivering<'_>,
) -> Result<Option<ItemId>, PortError> {
    let item_id = ItemId::new(delivering.ids.next())?;
    let body = serde_json::to_value(notice).map_err(|error| PortError::Unavailable {
        detail: error.to_string(),
    })?;
    let inserted = sqlx::query(
        "INSERT INTO inbox_items (item_id, kind, address, run_id, notice, accepted_at) \
         SELECT $1, 'NOTICE', $2, $3, $4, $5 \
         WHERE NOT EXISTS (SELECT 1 FROM directory_removed_runs d WHERE d.run_id = $3) \
           AND ($2::uuid IS NULL OR EXISTS (SELECT 1 FROM directory_agents a WHERE a.agent_id = $2))",
    )
    .bind(item_id.as_uuid())
    .bind(target.address.map(|address| address.as_uuid()))
    .bind(target.run_id.as_uuid())
    .bind(body)
    .bind(delivering.at)
    .execute(&mut *tx)
    .await
    .map_err(unavailable)?;
    if inserted.rows_affected() == 0 {
        return Ok(None);
    }
    let live: Vec<Uuid> = sqlx::query_scalar(LIVE_RUNS_REACHED_SQL)
        .bind(target.run_id.as_uuid())
        .fetch_all(&mut *tx)
        .await
        .map_err(unavailable)?;
    for run in live {
        ring(
            &mut *tx,
            Arrival {
                run_id: RunId::new(run)?,
                address: target.address.map(|address| address.as_uuid()),
                item_id,
                kind: ItemKind::Notice,
            },
            Ring {
                record_id: delivering.ids.next(),
                event_id: delivering.ids.next(),
                metadata: delivering.metadata,
                at: delivering.at,
            },
        )
        .await?;
    }
    Ok(Some(item_id))
}
