use crate::domain::actions::{Affordance, Availability};
use crate::domain::ids::JobId;
use crate::domain::job::Job;
use crate::domain::job::status::JobStatus;
use crate::error::JobsError;

pub const CANCEL: &str = "cancel";
pub const MANUAL_RETRY: &str = "manual_retry";
pub const DELETE: &str = "delete";

pub fn guard_parent_admits_work(
    parent_job_id: JobId,
    parent: Option<&Job>,
) -> Result<&Job, JobsError> {
    let parent = parent
        .filter(|candidate| candidate.id() == parent_job_id)
        .ok_or(JobsError::ParentJobUnknown {
            parent_job_id: parent_job_id.as_uuid(),
        })?;
    if parent.is_deleted() {
        return Err(JobsError::ParentJobDeleted {
            parent_job_id: parent_job_id.as_uuid(),
        });
    }
    if parent.is_terminal() {
        return Err(JobsError::ParentJobTerminal {
            parent_job_id: parent_job_id.as_uuid(),
        });
    }
    Ok(parent)
}

impl Job {
    pub fn guard_cancel(&self) -> Result<(), JobsError> {
        self.guard_not_deleted()?;
        self.guard_not_terminal()
    }

    pub fn guard_manual_retry(&self, parent: Option<&Job>) -> Result<(), JobsError> {
        self.guard_not_deleted()?;
        if self.status() != JobStatus::Failed {
            return Err(JobsError::JobNotFailed {
                status: self.status().as_db_str(),
            });
        }
        if let Some(record) = self.manual_retry() {
            return Err(JobsError::ManualRetryAlreadyStarted {
                successor_job_id: record.successor_job_id().as_uuid(),
            });
        }
        if let Some(parent_job_id) = self.parent_job_id() {
            guard_parent_admits_work(parent_job_id, parent)?;
        }
        Ok(())
    }

    pub fn guard_delete(&self) -> Result<(), JobsError> {
        if self.is_deleted() {
            return Err(JobsError::JobAlreadyDeleted);
        }
        self.guard_terminal()?;
        match self.manual_retry() {
            Some(record) => record.guard_successor_settled(),
            None => Ok(()),
        }
    }

    pub fn can_cancel(&self) -> Availability {
        Availability::from_guard(self.guard_cancel())
    }

    pub fn can_manual_retry(&self, parent: Option<&Job>) -> Availability {
        Availability::from_guard(self.guard_manual_retry(parent))
    }

    pub fn can_delete(&self) -> Availability {
        Availability::from_guard(self.guard_delete())
    }

    pub fn affordances(&self, parent: Option<&Job>) -> Vec<Affordance> {
        vec![
            Affordance::new(CANCEL, self.can_cancel()),
            Affordance::new(MANUAL_RETRY, self.can_manual_retry(parent)),
            Affordance::new(DELETE, self.can_delete()),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::job::resolution::{JobFailureCause, JobResolution};
    use crate::domain::run::failure::RunFailureKind;
    use crate::fixtures::{JobBuilder, RunBuilder, job_id, resolution_id, ts};
    use serde_json::json;

    fn failed_job() -> JobBuilder {
        JobBuilder::new()
            .with_run(
                RunBuilder::new(1)
                    .started(ts(5))
                    .failed(ts(20), RunFailureKind::Permanent)
                    .build(),
            )
            .with_resolution(
                JobResolution::failed(
                    resolution_id(),
                    ts(30),
                    JobFailureCause::DeclaredByOwner,
                    None,
                )
                .unwrap(),
            )
    }

    #[test]
    fn a_live_job_may_be_cancelled_but_neither_retried_nor_deleted() {
        // Given: a job in progress
        let job = JobBuilder::new()
            .with_run(RunBuilder::new(1).build())
            .build();
        // When: the backend computes what may be done to it
        // Then: cancel is open, the terminal-only actions are blocked with their codes
        assert_eq!(job.can_cancel(), Availability::Available);
        assert_eq!(
            job.can_manual_retry(None).reason_code(),
            Some("job_not_failed")
        );
        assert_eq!(job.can_delete().reason_code(), Some("job_not_terminal"));
    }

    #[test]
    fn a_failed_job_may_be_retried_and_deleted_but_no_longer_cancelled() {
        // Given: a job that failed and was never retried
        let job = failed_job().build();
        // When/Then: the terminal actions open and cancel closes
        assert_eq!(job.can_manual_retry(None), Availability::Available);
        assert_eq!(job.can_delete(), Availability::Available);
        assert_eq!(job.can_cancel().reason_code(), Some("job_already_terminal"));
    }

    #[test]
    fn a_failed_job_already_retried_may_not_be_retried_again() {
        // Given: a failed job whose manual-retry successor exists
        let successor = job_id();
        let job = failed_job().manually_retried_by(successor, false).build();
        // When/Then: the second retry is blocked, naming the successor in params
        assert_eq!(
            job.can_manual_retry(None),
            Availability::Blocked {
                reason_code: "manual_retry_already_started".to_owned(),
                params: json!({ "successorJobId": successor.as_uuid() })
            }
        );
    }

    #[test]
    fn a_failed_child_may_be_retried_while_the_parent_that_owns_it_still_runs() {
        // Given: a failed child job whose parent is still executing
        let parent = JobBuilder::new()
            .with_run(RunBuilder::new(1).started(ts(5)).build())
            .build();
        let child = failed_job().with_parent(parent.id()).build();
        // When/Then: the retry is open, judged against the parent that owns the child
        assert_eq!(
            child.can_manual_retry(Some(&parent)),
            Availability::Available
        );
    }

    #[test]
    fn a_failed_child_is_never_retried_under_a_parent_that_has_finished() {
        // Given: a failed child whose parent failed after it
        let parent = failed_job().build();
        let child = failed_job().with_parent(parent.id()).build();
        // When: the affordance and the command guard are both consulted
        let verdict = child.can_manual_retry(Some(&parent));
        // Then: both refuse — a successor under a finished parent could never be resolved
        assert_eq!(
            child.guard_manual_retry(Some(&parent)),
            Err(JobsError::ParentJobTerminal {
                parent_job_id: parent.id().as_uuid()
            })
        );
        assert_eq!(verdict.reason_code(), Some("parent_job_terminal"));
    }

    #[test]
    fn deletion_waits_while_the_manual_retry_successor_is_still_running() {
        // Given: a failed predecessor whose successor has not finished
        let successor = job_id();
        let job = failed_job().manually_retried_by(successor, false).build();
        // When/Then: deletion is blocked until the chain settles
        assert_eq!(
            job.can_delete(),
            Availability::Blocked {
                reason_code: "successor_still_active".to_owned(),
                params: json!({ "successorJobId": successor.as_uuid() })
            }
        );
    }

    #[test]
    fn deletion_opens_once_the_manual_retry_successor_is_terminal() {
        // Given: a failed predecessor whose successor has finished
        let job = failed_job().manually_retried_by(job_id(), true).build();
        // When/Then: the audit record may now be soft-deleted
        assert_eq!(job.can_delete(), Availability::Available);
    }

    #[test]
    fn a_deleted_job_offers_no_action_at_all() {
        // Given: an already soft-deleted job
        let job = failed_job().deleted(ts(90)).build();
        // When: the affordance surface is projected
        let affordances = job.affordances(None);
        // Then: every action is blocked, each with its own code
        assert_eq!(
            affordances
                .iter()
                .map(|entry| (entry.action(), entry.reason_code()))
                .collect::<Vec<_>>(),
            vec![
                (CANCEL, Some("job_deleted")),
                (MANUAL_RETRY, Some("job_deleted")),
                (DELETE, Some("job_already_deleted")),
            ]
        );
    }

    #[test]
    fn the_affordance_surface_names_every_administrator_action() {
        // Given: any job
        let job = JobBuilder::new().build();
        // When: the surface is projected for a snapshot or an event
        let actions: Vec<&str> = job
            .affordances(None)
            .iter()
            .map(|entry| entry.action())
            .collect();
        // Then: the three administrator mutations are always present
        assert_eq!(actions, vec![CANCEL, MANUAL_RETRY, DELETE]);
    }

    #[test]
    fn the_affordance_and_the_command_refusal_are_the_same_verdict() {
        // Given: a job that may not be deleted
        let job = JobBuilder::new().build();
        // When: the guard and the affordance are both consulted
        let refusal = job.guard_delete().unwrap_err();
        // Then: the client sees exactly the code the mutation would return
        assert_eq!(job.can_delete().reason_code(), Some(refusal.code()));
    }
}
