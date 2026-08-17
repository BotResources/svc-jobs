use async_trait::async_trait;
use bc_jobs::domain::ids::{JobId, RunId, RunLogId};
use bc_jobs::domain::keys::LogMessage;
use bc_jobs::domain::log::{RunLogLevel, RunLogLine};
use bc_jobs::ports::PortError;
use bc_jobs::ports::log::RunLogWriter;
use chrono::{DateTime, Utc};
use sqlx::Row;
use uuid::Uuid;

use super::PgStore;
use super::hydrate::unavailable;
use super::notify;

const COLUMNS: &str = "l.id::uuid AS id, r.job_id::uuid AS job_id, l.run_id::uuid AS run_id, \
     l.step_index, l.level::text AS level, l.message, l.logged_at";

pub struct LogPage {
    pub lines: Vec<(JobId, RunLogLine)>,
    pub has_next_page: bool,
    pub has_previous_page: bool,
}

#[derive(Clone, Copy)]
pub struct LogWindow {
    pub first: Option<i64>,
    pub last: Option<i64>,
}

pub fn cursor_of(line: &RunLogLine) -> String {
    format!(
        "{}|{}",
        line.logged_at().timestamp_micros(),
        line.id().as_uuid()
    )
}

pub fn parse_cursor(cursor: &str) -> Option<(DateTime<Utc>, Uuid)> {
    let (micros, id) = cursor.split_once('|')?;
    let micros: i64 = micros.parse().ok()?;
    Some((
        DateTime::from_timestamp_micros(micros)?,
        Uuid::parse_str(id).ok()?,
    ))
}

impl PgStore {
    pub async fn log_page(
        &self,
        job_id: JobId,
        run_id: Option<RunId>,
        window: LogWindow,
        after: Option<&str>,
        before: Option<&str>,
    ) -> Result<LogPage, PortError> {
        let backwards = window.last.is_some() && window.first.is_none();
        let limit = window.last.or(window.first).unwrap_or(200).clamp(1, 500);
        let order = if backwards {
            "ORDER BY l.logged_at DESC, l.id DESC"
        } else {
            "ORDER BY l.logged_at ASC, l.id ASC"
        };
        let boundary = if backwards {
            "($4::timestamptz IS NULL OR (l.logged_at, l.id::uuid) < ($4, $5::uuid))"
        } else {
            "($4::timestamptz IS NULL OR (l.logged_at, l.id::uuid) > ($4, $5::uuid))"
        };
        let sql = format!(
            "SELECT {COLUMNS} FROM run_logs l JOIN runs r ON r.id = l.run_id \
             WHERE r.job_id = $1 AND ($2::uuid IS NULL OR l.run_id = $2) AND {boundary} \
             {order} LIMIT $3"
        );
        let edge = if backwards { before } else { after };
        let (at, id) = edge
            .and_then(parse_cursor)
            .map_or((None, None), |(at, id)| (Some(at), Some(id)));
        let rows = sqlx::query(&sql)
            .bind(job_id.as_uuid())
            .bind(run_id.map(|run| run.as_uuid()))
            .bind(limit + 1)
            .bind(at)
            .bind(id)
            .fetch_all(self.pool())
            .await
            .map_err(unavailable)?;
        let more = rows.len() as i64 > limit;
        let mut lines = Vec::new();
        for row in rows.iter().take(limit as usize) {
            lines.push(line_of(row)?);
        }
        if backwards {
            lines.reverse();
        }
        Ok(LogPage {
            lines,
            has_next_page: !backwards && more,
            has_previous_page: backwards && more,
        })
    }

    pub async fn log_line(&self, id: RunLogId) -> Result<Option<(JobId, RunLogLine)>, PortError> {
        let sql = format!(
            "SELECT {COLUMNS} FROM run_logs l JOIN runs r ON r.id = l.run_id WHERE l.id = $1"
        );
        let row = sqlx::query(&sql)
            .bind(id.as_uuid())
            .fetch_optional(self.pool())
            .await
            .map_err(unavailable)?;
        row.as_ref().map(line_of).transpose()
    }
}

fn line_of(row: &sqlx::postgres::PgRow) -> Result<(JobId, RunLogLine), PortError> {
    let step_index = row
        .get::<Option<i32>, _>("step_index")
        .map(|index| bc_jobs::domain::run::step::StepIndex::new(u32::try_from(index).unwrap_or(0)));
    Ok((
        JobId::new(row.get("job_id"))?,
        RunLogLine::new(
            RunLogId::new(row.get("id"))?,
            RunId::new(row.get("run_id"))?,
            step_index,
            RunLogLevel::from_db_str(&row.get::<String, _>("level"))?,
            LogMessage::new(row.get::<String, _>("message"))?,
            row.get("logged_at"),
        ),
    ))
}

#[async_trait]
impl RunLogWriter for PgStore {
    async fn append(&self, lines: &[RunLogLine]) -> Result<(), PortError> {
        let mut tx = self.begin().await?;
        for line in lines {
            let written = insert(&mut tx, line).await?;
            if let Some(job_id) = written {
                notify::run_log(&mut tx, line.id(), job_id, line.run_id()).await?;
            }
        }
        tx.commit().await.map_err(unavailable)?;
        Ok(())
    }
}

async fn insert(
    executor: &mut sqlx::PgConnection,
    line: &RunLogLine,
) -> Result<Option<JobId>, PortError> {
    let row = sqlx::query(
        "WITH inserted AS ( \
            INSERT INTO run_logs (id, run_id, step_index, level, message, logged_at) \
            SELECT $1, $2, $3, $4, $5, $6 \
            WHERE NOT EXISTS (SELECT 1 FROM run_logs existing WHERE existing.id = $1) \
            RETURNING run_id) \
         SELECT r.job_id::uuid AS job_id FROM inserted JOIN runs r ON r.id = inserted.run_id",
    )
    .bind(line.id().as_uuid())
    .bind(line.run_id().as_uuid())
    .bind(
        line.step_index()
            .map(|index| i32::try_from(index.get()).unwrap_or(i32::MAX)),
    )
    .bind(line.level().as_db_str())
    .bind(line.message().as_str())
    .bind(line.logged_at())
    .fetch_optional(&mut *executor)
    .await
    .map_err(unavailable)?;
    row.map(|row| JobId::new(row.get("job_id")).map_err(PortError::from))
        .transpose()
}
