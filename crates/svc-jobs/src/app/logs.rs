use bc_jobs::commands::log::{AppendRunLog, accept_log_line};
use bc_jobs::domain::ids::{RunId, RunLogId};
use bc_jobs::domain::keys::LogMessage;
use bc_jobs::domain::log::RunLogLevel;
use bc_jobs::domain::run::step::StepIndex;
use bc_jobs::ports::log::RunLogWriter;
use contract_jobs::runner as wire;

use super::Jobs;
use crate::error::ServiceError;

pub async fn append(jobs: &Jobs, line: &wire::LogLine) -> Result<(), ServiceError> {
    let Some(job_id) = jobs.store.job_of_run(line.run_id).await? else {
        tracing::debug!(run_id = %line.run_id, "log line for an unknown run, discarded");
        return Ok(());
    };
    let Some(job) = jobs.load(job_id).await? else {
        return Ok(());
    };
    let command = AppendRunLog {
        id: RunLogId::new(line.id.unwrap_or_else(|| jobs.ids.next()))?,
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
