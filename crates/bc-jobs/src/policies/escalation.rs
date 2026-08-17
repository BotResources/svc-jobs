use crate::domain::ids::JobId;
use crate::domain::job::resolution::JobFailureCause;
use crate::domain::ownership::JobOwner;
use crate::domain::run::failure::RunFailureReport;
use crate::event::job_facts::JobFailed;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailureEscalation {
    pub failed_job_id: JobId,
    pub owner: JobOwner,
    pub failure_cause: JobFailureCause,
    pub report: Option<RunFailureReport>,
}

pub fn escalate_failure(owner: &JobOwner, failed: &JobFailed) -> FailureEscalation {
    FailureEscalation {
        failed_job_id: failed.job_id,
        owner: owner.clone(),
        failure_cause: failed.failure_cause,
        report: failed.report.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::run::failure::RunFailureKind;
    use crate::fixtures::{job_id, producer, report, resolution_id, run_id, runner_type};

    fn failed_child(parent_job_id: JobId) -> (JobOwner, JobFailed) {
        (
            JobOwner::Runner {
                parent_job_id,
                runner_type: runner_type(),
            },
            JobFailed {
                job_id: job_id(),
                resolution_id: resolution_id(),
                failure_cause: JobFailureCause::TerminalRunFailure,
                caused_by_run_id: Some(run_id()),
                report: Some(report(RunFailureKind::Permanent)),
            },
        )
    }

    #[test]
    fn a_child_failure_is_escalated_to_the_runner_that_owns_it() {
        // Given: a child job that failed under a parent
        let parent = job_id();
        let (owner, failed) = failed_child(parent);
        // When: the failure is escalated
        let escalation = escalate_failure(&owner, &failed);
        // Then: the owner receives it with the report forwarded unchanged
        assert_eq!(escalation.owner.parent_job_id(), Some(parent));
        assert_eq!(escalation.report, failed.report);
    }

    #[test]
    fn escalation_carries_information_and_decides_nothing_for_the_owner() {
        // Given: a root job owned by its producer
        let owner = JobOwner::Producer(producer());
        let failed = JobFailed {
            job_id: job_id(),
            resolution_id: resolution_id(),
            failure_cause: JobFailureCause::InactivityTimeout,
            caused_by_run_id: None,
            report: None,
        };
        // When: the failure is escalated
        let escalation = escalate_failure(&owner, &failed);
        // Then: the producer learns the cause; what happens next is its own call
        assert_eq!(escalation.failure_cause, JobFailureCause::InactivityTimeout);
        assert_eq!(escalation.report, None);
    }
}
