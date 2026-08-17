use bc_jobs::commands::log::{AppendRunLog, accept_log_line};
use bc_jobs::domain::ids::{RunId, RunLogId};
use bc_jobs::domain::keys::LogMessage;
use bc_jobs::domain::log::RunLogLevel;
use bc_jobs::domain::run::step::StepIndex;
use bc_jobs::ports::log::RunLogWriter;
use contract_jobs::runner as wire;
use uuid::{Builder, Uuid};

use super::Jobs;
use crate::error::ServiceError;

const FNV_OFFSET_BASIS: u128 = 0x6c62_272e_07bb_0142_62b8_2175_6295_c58d;
const FNV_PRIME: u128 = 0x0000_0000_0100_0000_0000_0000_0000_013b;

pub async fn append(jobs: &Jobs, line: &wire::LogLine) -> Result<(), ServiceError> {
    let Some(job_id) = jobs.store.job_of_run(line.run_id).await? else {
        tracing::debug!(run_id = %line.run_id, "log line for an unknown run, discarded");
        return Ok(());
    };
    let Some(job) = jobs.load(job_id).await? else {
        return Ok(());
    };
    let command = AppendRunLog {
        id: RunLogId::new(line.id.unwrap_or_else(|| identity_of(line)))?,
        run_id: RunId::new(line.run_id)?,
        step_index: line.step_index.map(StepIndex::new),
        level: RunLogLevel::from_db_str(&line.level)?,
        message: LogMessage::new(&line.message)?,
        logged_at: line.logged_at,
    };
    let accepted = accept_log_line(&job, command)?;
    RunLogWriter::append(&jobs.store, &[accepted]).await?;
    Ok(())
}

fn identity_of(line: &wire::LogLine) -> Uuid {
    let mut digest = FNV_OFFSET_BASIS;
    for byte in line
        .run_id
        .as_bytes()
        .iter()
        .chain(&line.step_index.unwrap_or(u32::MAX).to_be_bytes())
        .chain(line.level.as_bytes())
        .chain(
            &line
                .logged_at
                .timestamp_nanos_opt()
                .unwrap_or(0)
                .to_be_bytes(),
        )
        .chain(line.message.as_bytes())
    {
        digest ^= u128::from(*byte);
        digest = digest.wrapping_mul(FNV_PRIME);
    }
    let mut tail = [0u8; 10];
    tail.copy_from_slice(&digest.to_be_bytes()[..10]);
    Builder::from_unix_timestamp_millis(
        u64::try_from(line.logged_at.timestamp_millis()).unwrap_or(0),
        &tail,
    )
    .into_uuid()
}
