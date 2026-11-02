use bc_jobs::domain::auto_finish::{AnswerRecord, CallerRun, CallerRunEnd};
use bc_jobs::domain::ids::RunId;
use bc_jobs::domain::payload::CallMode;
use bc_jobs::domain::run::status::RunTerminalKind;
use bc_jobs::ports::PortError;
use chrono::{DateTime, Utc};
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use crate::db::hydrate::unavailable;

const SYNC_ANSWERS_SQL: &str = "SELECT t.run_id::uuid AS run_id, i.accepted_at AS recorded_at, \
        i.read_at AS read_at \
     FROM inbox_items i \
     JOIN runs r ON r.job_id = (i.notice ->> 'job_id')::uuid \
     JOIN run_terminals t ON t.run_id = r.id AND t.kind = 'COMPLETED' \
     WHERE i.kind = 'NOTICE' AND i.address IS NULL AND i.run_id = $1 \
       AND i.notice ->> 'type' = 'RUN_COMPLETED' \
       AND (i.notice ->> 'job_id')::uuid = ANY($2)";

const BACKGROUND_ANSWERS_SQL: &str = "SELECT DISTINCT ON (m.sender_run_id) \
        m.sender_run_id::uuid AS run_id, m.created_at AS recorded_at, i.read_at AS read_at \
     FROM mails m \
     JOIN mail_recipients mr ON mr.mail_id = m.mail_id AND mr.address = $1 \
        AND mr.kind = 'DIRECT' \
     LEFT JOIN inbox_items i ON i.mail_id = m.mail_id AND i.address = $1 AND i.kind = 'MAIL' \
     WHERE m.sender_run_id = ANY($2) \
     ORDER BY m.sender_run_id, m.created_at DESC, m.mail_id DESC";

const CALLER_RUNS_SQL: &str = "WITH RECURSIVE acting(run_id) AS ( \
        SELECT $1::uuid \
      UNION \
        SELECT r.id::uuid FROM acting a \
        JOIN restart_origins ro ON ro.restarts_run_id = a.run_id \
        JOIN runs r ON r.job_id = ro.job_id) \
     SELECT a.run_id, t.kind::text AS kind, t.occurred_at AS at \
     FROM acting a LEFT JOIN run_terminals t ON t.run_id = a.run_id";

pub struct AnswerSource {
    pub mode: CallMode,
    pub caller_run: Uuid,
    pub caller_address: Option<Uuid>,
    pub family_jobs: Vec<Uuid>,
    pub family_runs: Vec<Uuid>,
}

pub async fn answers_of(
    tx: &mut PgConnection,
    source: &AnswerSource,
) -> Result<Vec<AnswerRecord>, PortError> {
    let rows = match (source.mode, source.caller_address) {
        (CallMode::Sync, _) => {
            sqlx::query(SYNC_ANSWERS_SQL)
                .bind(source.caller_run)
                .bind(&source.family_jobs)
                .fetch_all(&mut *tx)
                .await
        }
        (CallMode::Background, Some(address)) => {
            sqlx::query(BACKGROUND_ANSWERS_SQL)
                .bind(address)
                .bind(&source.family_runs)
                .fetch_all(&mut *tx)
                .await
        }
        (CallMode::Background, None) => return Ok(Vec::new()),
    }
    .map_err(unavailable)?;
    rows.iter()
        .map(|row| {
            let recorded_at: DateTime<Utc> = row.get("recorded_at");
            Ok(AnswerRecord {
                run_id: RunId::new(row.get("run_id"))?,
                recorded_at,
                read_at: row.get("read_at"),
            })
        })
        .collect()
}

pub async fn caller_runs_of(
    tx: &mut PgConnection,
    caller_run: Uuid,
) -> Result<Vec<CallerRun>, PortError> {
    let rows = sqlx::query(CALLER_RUNS_SQL)
        .bind(caller_run)
        .fetch_all(&mut *tx)
        .await
        .map_err(unavailable)?;
    rows.iter()
        .map(|row| {
            let kind: Option<String> = row.get("kind");
            let at: Option<DateTime<Utc>> = row.get("at");
            let ended = match (kind, at) {
                (Some(kind), Some(at)) => Some(CallerRunEnd {
                    kind: RunTerminalKind::from_db_str(&kind)?,
                    at,
                }),
                _ => None,
            };
            Ok(CallerRun {
                run_id: RunId::new(row.get("run_id"))?,
                ended,
            })
        })
        .collect()
}
