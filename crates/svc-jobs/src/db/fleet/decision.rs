use std::collections::HashMap;

use bc_jobs::domain::actions::fleet::{RetirementWindow, RunnerTypeDecisionFacts};
use bc_jobs::domain::keys::RunnerTypeKey;
use bc_jobs::ports::PortError;
use chrono::{DateTime, Utc};
use sqlx::{PgConnection, Row};

use crate::db::PgStore;
use crate::db::hydrate::unavailable;

const FACTS_SQL: &str = "SELECT rt.type_key, \
     (SELECT count(*) FROM jobs j \
        WHERE j.runner_type_id = rt.id \
          AND NOT EXISTS (SELECT 1 FROM job_resolutions jr WHERE jr.job_id = j.id) \
     )::bigint AS active_count, \
     (SELECT max(term.occurred_at) FROM run_terminals term \
        JOIN runs r ON r.id = term.run_id \
        JOIN jobs jt ON jt.id = r.job_id \
        WHERE jt.runner_type_id = rt.id \
     ) AS latest_terminal_run_at \
     FROM runner_types rt \
     JOIN registered_runner_types registered ON registered.runner_type_id = rt.id \
     WHERE rt.type_key = ANY($1)";

const TERMINALS_SQL: &str = "SELECT rt.type_key, \
     (SELECT max(term.occurred_at) FROM run_terminals term \
        JOIN runs r ON r.id = term.run_id \
        JOIN jobs jt ON jt.id = r.job_id \
        WHERE jt.runner_type_id = rt.id \
     ) AS latest_terminal_run_at \
     FROM runner_types rt \
     JOIN registered_runner_types registered ON registered.runner_type_id = rt.id \
     WHERE rt.type_key = ANY($1)";

pub async fn terminals_for(
    connection: &mut PgConnection,
    keys: &[RunnerTypeKey],
) -> Result<HashMap<RunnerTypeKey, Option<DateTime<Utc>>>, PortError> {
    if keys.is_empty() {
        return Ok(HashMap::new());
    }
    let raw: Vec<&str> = keys.iter().map(RunnerTypeKey::as_str).collect();
    let rows = sqlx::query(TERMINALS_SQL)
        .bind(&raw)
        .fetch_all(&mut *connection)
        .await
        .map_err(unavailable)?;
    rows.iter()
        .map(|row| {
            Ok((
                RunnerTypeKey::new(row.get::<String, _>("type_key"))?,
                row.get("latest_terminal_run_at"),
            ))
        })
        .collect()
}

pub async fn facts_of(
    connection: &mut PgConnection,
    key: &RunnerTypeKey,
    window: RetirementWindow,
) -> Result<RunnerTypeDecisionFacts, PortError> {
    let row = sqlx::query(FACTS_SQL)
        .bind(&[key.as_str()][..])
        .fetch_optional(&mut *connection)
        .await
        .map_err(unavailable)?
        .ok_or(PortError::ConcurrentModification)?;
    let active_count: i64 = row.get("active_count");
    Ok(RunnerTypeDecisionFacts {
        non_terminal_job_count: u32::try_from(active_count).unwrap_or(u32::MAX),
        latest_terminal_run_at: row.get("latest_terminal_run_at"),
        window,
    })
}

impl PgStore {
    pub async fn runner_type_decision_facts_in(
        tx: &mut PgConnection,
        key: &RunnerTypeKey,
        window: RetirementWindow,
    ) -> Result<RunnerTypeDecisionFacts, PortError> {
        facts_of(tx, key, window).await
    }
}
