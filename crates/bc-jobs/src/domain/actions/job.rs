use crate::domain::access::PrincipalFacts;
use crate::domain::actions::{Affordance, Availability};
use crate::domain::job::Job;
use crate::domain::job::parenting::{ParentContext, guard_admits_work};
use crate::domain::job::status::JobStatus;
use crate::error::JobsError;

pub const CANCEL: &str = "cancel";
pub const MANUAL_RETRY: &str = "manual_retry";
pub const DELETE: &str = "delete";
pub const FORCE_PRIORITY: &str = "force_priority";

impl Job {
    pub fn guard_cancel(&self) -> Result<(), JobsError> {
        self.guard_not_deleted()?;
        self.guard_not_terminal()
    }

    pub fn guard_manual_retry(&self, parent: ParentContext<'_>) -> Result<(), JobsError> {
        self.guard_manual_retry_under(parent.resolve(self.parent_job_id())?)
    }

    fn guard_manual_retry_under(&self, parent: Option<&Job>) -> Result<(), JobsError> {
        self.guard_not_deleted()?;
        if self.status() != JobStatus::Failed {
            return Err(JobsError::JobNotFailed {
                status: self.status().as_db_str(),
            });
        }
        if self.tree_scrubbed() {
            return Err(JobsError::TreeScrubbed);
        }
        if let Some(record) = self.manual_retry() {
            return Err(JobsError::ManualRetryAlreadyStarted {
                successor_job_id: record.successor_job_id().as_uuid(),
            });
        }
        if let Some(parent) = parent {
            guard_admits_work(parent)?;
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

    pub fn guard_force_priority(&self, principal: &PrincipalFacts) -> Result<(), JobsError> {
        principal.guard_super_user()?;
        self.guard_not_deleted()?;
        self.guard_not_terminal()
    }

    pub fn can_force_priority(&self, principal: &PrincipalFacts) -> Availability {
        Availability::from_guard(self.guard_force_priority(principal))
    }

    pub fn can_cancel(&self) -> Availability {
        Availability::from_guard(self.guard_cancel())
    }

    pub fn can_manual_retry(&self, parent: ParentContext<'_>) -> Result<Availability, JobsError> {
        let parent = parent.resolve(self.parent_job_id())?;
        Ok(Availability::from_guard(
            self.guard_manual_retry_under(parent),
        ))
    }

    pub fn can_delete(&self) -> Availability {
        Availability::from_guard(self.guard_delete())
    }

    pub fn affordances(
        &self,
        parent: ParentContext<'_>,
        principal: &PrincipalFacts,
    ) -> Result<Vec<Affordance>, JobsError> {
        Ok(vec![
            Affordance::new(CANCEL, self.can_cancel()),
            Affordance::new(MANUAL_RETRY, self.can_manual_retry(parent)?),
            Affordance::new(DELETE, self.can_delete()),
            Affordance::new(FORCE_PRIORITY, self.can_force_priority(principal)),
        ])
    }
}

#[cfg(test)]
mod tests;
