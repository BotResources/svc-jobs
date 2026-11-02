use bc_jobs::domain::erasure::{ERASED_CONVERSATION_ID, ERASED_TEXT};
use bc_jobs::domain::ids::JobId;
use bc_jobs::ports::PortError;
use chrono::{DateTime, Utc};
use sqlx::postgres::PgArguments;
use sqlx::query::Query;
use sqlx::{PgConnection, Postgres};
use uuid::Uuid;

use crate::db::hydrate::unavailable;

type Statement<'q> = Query<'q, Postgres, PgArguments>;

async fn exec(tx: &mut PgConnection, statement: Statement<'_>) -> Result<(), PortError> {
    statement.execute(&mut *tx).await.map_err(unavailable)?;
    Ok(())
}

pub async fn erase_request_data(
    tx: &mut PgConnection,
    jobs: &[Uuid],
    at: DateTime<Utc>,
) -> Result<(), PortError> {
    exec(
        tx,
        sqlx::query(
            "UPDATE jobs SET config = NULL, passport = NULL, payload = NULL, \
                content_erased_at = coalesce(content_erased_at, $2) \
             WHERE id = ANY($1)",
        )
        .bind(jobs)
        .bind(at),
    )
    .await?;
    exec(
        tx,
        sqlx::query("DELETE FROM kept_payloads WHERE job_id = ANY($1)").bind(jobs),
    )
    .await
}

pub async fn erase_results(
    tx: &mut PgConnection,
    jobs: &[Uuid],
    runs: &[Uuid],
) -> Result<(), PortError> {
    exec(
        tx,
        sqlx::query("DELETE FROM job_results WHERE job_id = ANY($1) OR addressed_run_id = ANY($2)")
            .bind(jobs)
            .bind(runs),
    )
    .await
}

pub async fn erase_mail_content(
    tx: &mut PgConnection,
    initial_job_id: JobId,
    at: DateTime<Utc>,
) -> Result<(), PortError> {
    let tree = initial_job_id.as_uuid();
    exec(
        tx,
        sqlx::query(
            "DELETE FROM mail_attachments \
             WHERE mail_id IN (SELECT mail_id FROM mails WHERE initial_job_id = $1)",
        )
        .bind(tree),
    )
    .await?;
    exec(
        tx,
        sqlx::query(
            "UPDATE mails SET author = $2, source_conversation_id = $3, subject = '', \
                body = '', content_erased_at = coalesce(content_erased_at, $4) \
             WHERE initial_job_id = $1",
        )
        .bind(tree)
        .bind(ERASED_TEXT)
        .bind(ERASED_CONVERSATION_ID)
        .bind(at),
    )
    .await
}

pub async fn erase_items(
    tx: &mut PgConnection,
    initial_job_id: JobId,
    runs: &[Uuid],
) -> Result<(), PortError> {
    exec(
        tx,
        sqlx::query(
            "DELETE FROM inbox_items WHERE item_id IN ( \
                SELECT item_id FROM inbox_items WHERE run_id = ANY($2) \
                UNION \
                SELECT i.item_id FROM inbox_items i JOIN mails m ON m.mail_id = i.mail_id \
                    WHERE m.initial_job_id = $1 \
                UNION \
                SELECT i.item_id FROM inbox_items i \
                    JOIN directory_agents d ON d.agent_id = i.address \
                    WHERE d.initial_job_id = $1)",
        )
        .bind(initial_job_id.as_uuid())
        .bind(runs),
    )
    .await
}

pub async fn erase_saved_state(tx: &mut PgConnection, runs: &[Uuid]) -> Result<(), PortError> {
    exec(
        tx,
        sqlx::query("DELETE FROM saved_states WHERE run_id = ANY($1)").bind(runs),
    )
    .await?;
    exec(
        tx,
        sqlx::query("DELETE FROM restart_wakes WHERE run_id = ANY($1)").bind(runs),
    )
    .await
}

pub async fn erase_run_records(tx: &mut PgConnection, runs: &[Uuid]) -> Result<(), PortError> {
    exec(
        tx,
        sqlx::query("UPDATE run_logs SET message = $2 WHERE run_id = ANY($1) AND message <> $2")
            .bind(runs)
            .bind(ERASED_TEXT),
    )
    .await?;
    exec(
        tx,
        sqlx::query("UPDATE steps SET label = $2 WHERE run_id = ANY($1) AND label <> $2")
            .bind(runs)
            .bind(ERASED_TEXT),
    )
    .await?;
    exec(
        tx,
        sqlx::query(
            "UPDATE run_plan_items SET label = $2 \
             WHERE declaration_id IN \
                (SELECT id FROM run_plan_declarations WHERE run_id = ANY($1)) \
               AND label <> $2",
        )
        .bind(runs)
        .bind(ERASED_TEXT),
    )
    .await?;
    exec(
        tx,
        sqlx::query(
            "UPDATE run_terminals SET params = '{}'::jsonb, diagnostic = '{}'::jsonb \
             WHERE run_id = ANY($1) AND kind = 'FAILED'",
        )
        .bind(runs),
    )
    .await
}
