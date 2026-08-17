use serde::Serialize;
use serde_json::Value;

use crate::domain::ids::JobId;
use crate::error::JobsError;
use crate::event::job_facts::{
    JobAffordancesChanged, JobCancelled, JobCompleted, JobDeleted, JobFailed, JobQueued,
    ManualRetryStarted, RetryScheduled, RunCancellationRequested, RunCancelled, RunCompleted,
    RunDispatched, RunFailed, RunPlanDeclared, RunStarted, RunStepStarted,
};

pub const JOB_AGGREGATE_TYPE: &str = "Job";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobEvent {
    JobQueued(JobQueued),
    RunDispatched(RunDispatched),
    RunStarted(RunStarted),
    RunPlanDeclared(RunPlanDeclared),
    RunStepStarted(RunStepStarted),
    RunCompleted(RunCompleted),
    RunFailed(RunFailed),
    RunCancellationRequested(RunCancellationRequested),
    RunCancelled(RunCancelled),
    RetryScheduled(RetryScheduled),
    JobCompleted(JobCompleted),
    JobFailed(JobFailed),
    JobCancelled(JobCancelled),
    ManualRetryStarted(ManualRetryStarted),
    JobDeleted(JobDeleted),
    JobAffordancesChanged(JobAffordancesChanged),
}

fn payload_of<T: Serialize>(fact: &T) -> Result<Value, JobsError> {
    serde_json::to_value(fact).map_err(JobsError::from)
}

fn fact_of<T: serde::de::DeserializeOwned>(payload: Value) -> Result<T, JobsError> {
    serde_json::from_value(payload).map_err(JobsError::from)
}

impl JobEvent {
    pub fn event_type(&self) -> &'static str {
        match self {
            Self::JobQueued(_) => "JobQueued",
            Self::RunDispatched(_) => "RunDispatched",
            Self::RunStarted(_) => "RunStarted",
            Self::RunPlanDeclared(_) => "RunPlanDeclared",
            Self::RunStepStarted(_) => "RunStepStarted",
            Self::RunCompleted(_) => "RunCompleted",
            Self::RunFailed(_) => "RunFailed",
            Self::RunCancellationRequested(_) => "RunCancellationRequested",
            Self::RunCancelled(_) => "RunCancelled",
            Self::RetryScheduled(_) => "RetryScheduled",
            Self::JobCompleted(_) => "JobCompleted",
            Self::JobFailed(_) => "JobFailed",
            Self::JobCancelled(_) => "JobCancelled",
            Self::ManualRetryStarted(_) => "ManualRetryStarted",
            Self::JobDeleted(_) => "JobDeleted",
            Self::JobAffordancesChanged(_) => "JobAffordancesChanged",
        }
    }

    pub fn job_id(&self) -> JobId {
        match self {
            Self::JobQueued(fact) => fact.job_id,
            Self::RunDispatched(fact) => fact.job_id,
            Self::RunStarted(fact) => fact.job_id,
            Self::RunPlanDeclared(fact) => fact.job_id,
            Self::RunStepStarted(fact) => fact.job_id,
            Self::RunCompleted(fact) => fact.job_id,
            Self::RunFailed(fact) => fact.job_id,
            Self::RunCancellationRequested(fact) => fact.job_id,
            Self::RunCancelled(fact) => fact.job_id,
            Self::RetryScheduled(fact) => fact.job_id,
            Self::JobCompleted(fact) => fact.job_id,
            Self::JobFailed(fact) => fact.job_id,
            Self::JobCancelled(fact) => fact.job_id,
            Self::ManualRetryStarted(fact) => fact.job_id,
            Self::JobDeleted(fact) => fact.job_id,
            Self::JobAffordancesChanged(fact) => fact.job_id,
        }
    }

    pub fn payload(&self) -> Result<Value, JobsError> {
        match self {
            Self::JobQueued(fact) => payload_of(fact),
            Self::RunDispatched(fact) => payload_of(fact),
            Self::RunStarted(fact) => payload_of(fact),
            Self::RunPlanDeclared(fact) => payload_of(fact),
            Self::RunStepStarted(fact) => payload_of(fact),
            Self::RunCompleted(fact) => payload_of(fact),
            Self::RunFailed(fact) => payload_of(fact),
            Self::RunCancellationRequested(fact) => payload_of(fact),
            Self::RunCancelled(fact) => payload_of(fact),
            Self::RetryScheduled(fact) => payload_of(fact),
            Self::JobCompleted(fact) => payload_of(fact),
            Self::JobFailed(fact) => payload_of(fact),
            Self::JobCancelled(fact) => payload_of(fact),
            Self::ManualRetryStarted(fact) => payload_of(fact),
            Self::JobDeleted(fact) => payload_of(fact),
            Self::JobAffordancesChanged(fact) => payload_of(fact),
        }
    }

    pub fn decode(event_type: &str, payload: Value) -> Result<Self, JobsError> {
        match event_type {
            "JobQueued" => Ok(Self::JobQueued(fact_of(payload)?)),
            "RunDispatched" => Ok(Self::RunDispatched(fact_of(payload)?)),
            "RunStarted" => Ok(Self::RunStarted(fact_of(payload)?)),
            "RunPlanDeclared" => Ok(Self::RunPlanDeclared(fact_of(payload)?)),
            "RunStepStarted" => Ok(Self::RunStepStarted(fact_of(payload)?)),
            "RunCompleted" => Ok(Self::RunCompleted(fact_of(payload)?)),
            "RunFailed" => Ok(Self::RunFailed(fact_of(payload)?)),
            "RunCancellationRequested" => Ok(Self::RunCancellationRequested(fact_of(payload)?)),
            "RunCancelled" => Ok(Self::RunCancelled(fact_of(payload)?)),
            "RetryScheduled" => Ok(Self::RetryScheduled(fact_of(payload)?)),
            "JobCompleted" => Ok(Self::JobCompleted(fact_of(payload)?)),
            "JobFailed" => Ok(Self::JobFailed(fact_of(payload)?)),
            "JobCancelled" => Ok(Self::JobCancelled(fact_of(payload)?)),
            "ManualRetryStarted" => Ok(Self::ManualRetryStarted(fact_of(payload)?)),
            "JobDeleted" => Ok(Self::JobDeleted(fact_of(payload)?)),
            "JobAffordancesChanged" => Ok(Self::JobAffordancesChanged(fact_of(payload)?)),
            other => Err(JobsError::UnknownEnumValue {
                field: "job_event_type",
                value: other.to_owned(),
            }),
        }
    }

    pub fn changes_job_state(&self) -> bool {
        !matches!(self, Self::JobAffordancesChanged(_))
    }

    pub fn reaches_client(&self) -> bool {
        match self {
            Self::JobQueued(_)
            | Self::RunDispatched(_)
            | Self::RunStarted(_)
            | Self::RunPlanDeclared(_)
            | Self::RunStepStarted(_)
            | Self::RunCompleted(_)
            | Self::RunFailed(_)
            | Self::RunCancelled(_)
            | Self::RetryScheduled(_)
            | Self::JobCompleted(_)
            | Self::JobFailed(_)
            | Self::JobCancelled(_)
            | Self::ManualRetryStarted(_)
            | Self::JobDeleted(_)
            | Self::JobAffordancesChanged(_) => true,
            Self::RunCancellationRequested(_) => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::keys::ReasonCode;
    use crate::fixtures::{job_id, resolution_id, run_id};

    #[test]
    fn every_fact_addresses_the_job_it_belongs_to() {
        // Given: a fact about a run and a fact about the job itself
        let job = job_id();
        let run_fact = JobEvent::RunCancelled(RunCancelled {
            job_id: job,
            run_id: run_id(),
        });
        let job_fact = JobEvent::JobCompleted(JobCompleted {
            job_id: job,
            resolution_id: resolution_id(),
        });
        // When/Then: both are addressed to the aggregate root, never to the run
        assert_eq!(run_fact.job_id(), job);
        assert_eq!(job_fact.job_id(), job);
    }

    #[test]
    fn a_fact_serializes_the_data_a_subscriber_would_otherwise_re_query() {
        // Given: a completed run fact
        let event = JobEvent::RunCompleted(RunCompleted {
            job_id: job_id(),
            run_id: run_id(),
            attempt_number: crate::domain::attempts::AttemptNumber::FIRST,
        });
        // When: it is rendered for storage and transport
        let payload = event.payload().unwrap();
        // Then: the attempt number travels with it
        assert_eq!(event.event_type(), "RunCompleted");
        assert_eq!(payload["attempt_number"], 1);
    }

    #[test]
    fn the_stop_request_is_the_only_fact_the_client_never_sees() {
        // Given: the internal stop request and a fact of the same run's lifecycle
        let internal = JobEvent::RunCancellationRequested(RunCancellationRequested {
            job_id: job_id(),
            run_id: run_id(),
            reason_code: ReasonCode::new("job_cancelled").unwrap(),
            requested_by: None,
            originating_job_id: None,
        });
        let published = JobEvent::RunCancelled(RunCancelled {
            job_id: job_id(),
            run_id: run_id(),
        });
        // When/Then: only the orchestration fact is held back, and the domain owns that rule
        assert!(!internal.reaches_client());
        assert!(published.reaches_client());
    }

    #[test]
    fn an_affordance_change_is_the_only_fact_that_changes_no_job_state() {
        // Given: an affordance flip caused by another job settling
        let flip = JobEvent::JobAffordancesChanged(JobAffordancesChanged {
            job_id: job_id(),
            caused_by_job_id: Some(job_id()),
        });
        // When/Then: it is recognised as a pure affordance push
        assert!(!flip.changes_job_state());
        assert!(
            JobEvent::JobCompleted(JobCompleted {
                job_id: job_id(),
                resolution_id: resolution_id()
            })
            .changes_job_state()
        );
    }
}
