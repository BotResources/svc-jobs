use async_trait::async_trait;
use bc_jobs::domain::ids::{JobId, RunId};
use bc_jobs::ports::PortError;
use bc_jobs::ports::transport::TriggerSequence;
use bc_jobs::ports::trigger_ledger::{AwaitedRun, TriggerLedger};
use sqlx::Row;
use sqlx::postgres::PgRow;
use uuid::Uuid;

use super::PgStore;
use super::hydrate::unavailable;

const RECORD: &str = "INSERT INTO run_triggers (run_id, stream_sequence) VALUES ($1, $2) \
     ON CONFLICT (run_id) DO UPDATE SET stream_sequence = EXCLUDED.stream_sequence";

const AWAITING: &str = "SELECT r.job_id::uuid AS job_id, r.id::uuid AS run_id \
     FROM run_triggers t JOIN runs r ON r.id = t.run_id \
     WHERE t.stream_sequence = $1 \
     ORDER BY r.dispatched_at DESC, r.id DESC LIMIT 1";

#[async_trait]
impl TriggerLedger for PgStore {
    async fn record_trigger_sequence(
        &self,
        run_id: RunId,
        sequence: TriggerSequence,
    ) -> Result<(), PortError> {
        sqlx::query(RECORD)
            .bind(run_id.as_uuid())
            .bind(i64::try_from(sequence.get()).unwrap_or(i64::MAX))
            .execute(self.pool())
            .await
            .map_err(unavailable)?;
        Ok(())
    }

    async fn run_awaiting_trigger(
        &self,
        sequence: TriggerSequence,
    ) -> Result<Option<AwaitedRun>, PortError> {
        let row = sqlx::query(AWAITING)
            .bind(i64::try_from(sequence.get()).unwrap_or(i64::MAX))
            .fetch_optional(self.pool())
            .await
            .map_err(unavailable)?;
        row.map(|row| awaited(&row)).transpose()
    }
}

fn awaited(row: &PgRow) -> Result<AwaitedRun, PortError> {
    let job: Uuid = row.get("job_id");
    let run: Uuid = row.get("run_id");
    Ok(AwaitedRun {
        job_id: JobId::new(job)?,
        run_id: RunId::new(run)?,
    })
}
