use chrono::{DateTime, Utc};

use crate::domain::ids::{RunId, RunLogId};
use crate::domain::job::Job;
use crate::domain::keys::LogMessage;
use crate::domain::log::{RunLogLevel, RunLogLine};
use crate::domain::run::step::StepIndex;
use crate::error::JobsError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendRunLog {
    pub id: RunLogId,
    pub run_id: RunId,
    pub step_index: Option<StepIndex>,
    pub level: RunLogLevel,
    pub message: LogMessage,
    pub logged_at: DateTime<Utc>,
}

pub fn accept_log_line(job: &Job, command: AppendRunLog) -> Result<RunLogLine, JobsError> {
    job.find_run(command.run_id)?;
    Ok(RunLogLine::new(
        command.id,
        command.run_id,
        command.step_index,
        command.level,
        command.message,
        command.logged_at,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::job::resolution::JobResolution;
    use crate::fixtures::{JobBuilder, RunBuilder, resolution_id, run_id, ts};
    use uuid::Uuid;

    fn line(run_id: RunId, step_index: Option<u32>) -> AppendRunLog {
        AppendRunLog {
            id: RunLogId::new(Uuid::now_v7()).unwrap(),
            run_id,
            step_index: step_index.map(StepIndex::new),
            level: RunLogLevel::Info,
            message: LogMessage::new("connecting to provider").unwrap(),
            logged_at: ts(7),
        }
    }

    #[test]
    fn a_log_line_keeps_the_step_the_runner_associated_it_with() {
        // Given: a run and a runner that named the step its line belongs to
        let run = RunBuilder::new(1).started(ts(5)).build();
        let run_id = run.id();
        let job = JobBuilder::new().with_run(run).build();
        // When: the line is accepted
        let accepted = accept_log_line(&job, line(run_id, Some(2))).unwrap();
        // Then: the association is the runner's, never inferred from the timestamp
        assert_eq!(accepted.step_index(), Some(StepIndex::new(2)));
    }

    #[test]
    fn logs_are_still_accepted_after_the_job_reached_a_terminal_state() {
        // Given: a job that already failed, with its run's logs still arriving
        let run = RunBuilder::new(1).started(ts(5)).completed(ts(20)).build();
        let run_id = run.id();
        let job = JobBuilder::new()
            .with_run(run)
            .with_resolution(JobResolution::completed(resolution_id(), ts(30)))
            .build();
        // When: a late line arrives
        let accepted = accept_log_line(&job, line(run_id, None));
        // Then: the audit narrative is preserved — logs drive no lifecycle state
        assert!(accepted.is_ok());
    }

    #[test]
    fn a_line_for_a_run_of_another_job_is_refused() {
        // Given: a job with one run
        let job = JobBuilder::new()
            .with_run(RunBuilder::new(1).build())
            .build();
        let stranger = run_id();
        // When: a line arrives for a foreign run
        let result = accept_log_line(&job, line(stranger, None));
        // Then: it is refused rather than filed under the wrong job
        assert_eq!(
            result,
            Err(JobsError::RunNotFound {
                run_id: stranger.as_uuid()
            })
        );
    }
}
