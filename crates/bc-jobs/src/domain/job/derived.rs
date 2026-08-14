use chrono::{DateTime, Utc};

use crate::domain::attempts::MaxAttempts;
use crate::domain::ids::RunId;
use crate::domain::job::Job;
use crate::domain::job::status::JobStatus;
use crate::domain::ownership::Caller;
use crate::domain::policy::ServiceLimits;
use crate::domain::references::SourceReference;
use crate::domain::run::Run;
use crate::error::JobsError;

impl Job {
    pub fn status(&self) -> JobStatus {
        match self.resolution() {
            Some(resolution) => resolution.kind().as_status(),
            None if self.runs().is_empty() => JobStatus::Pending,
            None => JobStatus::InProgress,
        }
    }

    pub fn is_terminal(&self) -> bool {
        self.resolution().is_some()
    }

    pub fn is_deleted(&self) -> bool {
        self.deletion().is_some()
    }

    pub fn attempt_count(&self) -> u32 {
        u32::try_from(self.runs().len()).unwrap_or(u32::MAX)
    }

    pub fn active_run(&self) -> Option<&Run> {
        self.runs().iter().rev().find(|run| !run.is_terminal())
    }

    pub fn latest_run(&self) -> Option<&Run> {
        self.runs().last()
    }

    pub fn find_run(&self, id: RunId) -> Result<&Run, JobsError> {
        self.runs()
            .iter()
            .find(|run| run.id() == id)
            .ok_or(JobsError::RunNotFound {
                run_id: id.as_uuid(),
            })
    }

    pub fn unconsumed_retry(&self) -> Option<&Run> {
        self.runs().iter().rev().find(|run| {
            run.retry_schedule().is_some_and(|schedule| {
                !self
                    .runs()
                    .iter()
                    .any(|other| other.automatic_retry_schedule_id() == Some(schedule.id()))
            })
        })
    }

    pub fn next_attempt_at(&self) -> Option<DateTime<Utc>> {
        if self.is_terminal() {
            return None;
        }
        self.unconsumed_retry()
            .and_then(Run::retry_schedule)
            .map(|schedule| schedule.due_at())
    }

    pub fn source(&self) -> Option<SourceReference> {
        self.source_entity_id()
            .map(|entity_id| SourceReference::new(self.producer().clone(), entity_id))
    }

    pub fn has_predecessor(&self) -> bool {
        self.predecessor_job_id().is_some()
    }

    pub fn budget(&self, limits: &ServiceLimits) -> MaxAttempts {
        limits.budget(self.max_attempts())
    }

    pub fn last_activity_at(&self) -> DateTime<Utc> {
        let mut latest = self.created_at();
        for run in self.runs() {
            latest = latest.max(run.last_activity_at());
            if let Some(schedule) = run.retry_schedule() {
                latest = latest.max(schedule.due_at());
            }
        }
        if let Some(resolution) = self.resolution() {
            latest = latest.max(resolution.occurred_at());
        }
        latest
    }

    pub fn guard_not_deleted(&self) -> Result<(), JobsError> {
        if self.is_deleted() {
            Err(JobsError::JobDeleted)
        } else {
            Ok(())
        }
    }

    pub fn guard_not_terminal(&self) -> Result<(), JobsError> {
        if self.is_terminal() {
            Err(JobsError::JobAlreadyTerminal {
                status: self.status().as_db_str(),
            })
        } else {
            Ok(())
        }
    }

    pub fn guard_terminal(&self) -> Result<(), JobsError> {
        if self.is_terminal() {
            Ok(())
        } else {
            Err(JobsError::JobNotTerminal {
                status: self.status().as_db_str(),
            })
        }
    }

    pub fn guard_owner(&self, caller: &Caller) -> Result<(), JobsError> {
        if self.owner().authorizes(caller) {
            Ok(())
        } else {
            Err(JobsError::NotOwner)
        }
    }
}

#[cfg(test)]
mod tests;
