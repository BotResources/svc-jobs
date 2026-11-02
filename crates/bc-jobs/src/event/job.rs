use serde::Serialize;
use serde_json::Value;

use crate::domain::ids::JobId;
use crate::error::JobsError;
use crate::event::job_facts::{
    JobAffordancesChanged, JobCancelled, JobCompleted, JobContentErased, JobDeleted, JobFailed,
    JobPriorityForced, JobPriorityRecalculated, JobQueued, ManualRetryStarted, RetryScheduled,
    RunCancellationRequested, RunCancelled, RunCompleted, RunDispatched, RunFailed,
    RunPlanDeclared, RunStarted, RunStepStarted,
};

pub const JOB_AGGREGATE_TYPE: &str = "Job";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobEvent {
    JobQueued(Box<JobQueued>),
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
    JobContentErased(JobContentErased),
    JobPriorityForced(JobPriorityForced),
    JobPriorityRecalculated(JobPriorityRecalculated),
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
            Self::JobContentErased(_) => "JobContentErased",
            Self::JobPriorityForced(_) => "JobPriorityForced",
            Self::JobPriorityRecalculated(_) => "JobPriorityRecalculated",
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
            Self::JobContentErased(fact) => fact.job_id,
            Self::JobPriorityForced(fact) => fact.job_id,
            Self::JobPriorityRecalculated(fact) => fact.job_id,
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
            Self::JobContentErased(fact) => payload_of(fact),
            Self::JobPriorityForced(fact) => payload_of(fact),
            Self::JobPriorityRecalculated(fact) => payload_of(fact),
        }
    }

    pub fn decode(event_type: &str, payload: Value) -> Result<Self, JobsError> {
        match event_type {
            "JobQueued" => Ok(Self::JobQueued(Box::new(fact_of(payload)?))),
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
            "JobContentErased" => Ok(Self::JobContentErased(fact_of(payload)?)),
            "JobPriorityForced" => Ok(Self::JobPriorityForced(fact_of(payload)?)),
            "JobPriorityRecalculated" => Ok(Self::JobPriorityRecalculated(fact_of(payload)?)),
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
            | Self::JobAffordancesChanged(_)
            | Self::JobContentErased(_)
            | Self::JobPriorityForced(_)
            | Self::JobPriorityRecalculated(_) => true,
            Self::RunCancellationRequested(_) => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::ids::StartId;
    use crate::domain::keys::ReasonCode;
    use crate::fixtures::{job_id, resolution_id, run_id};
    use serde_json::json;
    use uuid::Uuid;

    fn stored_run_started(extra: Option<(&str, Value)>) -> Value {
        let mut payload = json!({
            "job_id": job_id().as_uuid(),
            "run_id": run_id().as_uuid(),
            "runner_type": "analyst",
            "instance_key": "pod-7",
        });
        if let Some((key, value)) = extra {
            payload[key] = value;
        }
        payload
    }

    #[test]
    fn a_run_started_stored_before_the_start_id_existed_still_decodes_without_one() {
        let decoded = JobEvent::decode("RunStarted", stored_run_started(None)).unwrap();
        match decoded {
            JobEvent::RunStarted(fact) => assert_eq!(fact.start_id, None),
            other => panic!("expected a RunStarted fact, got {other:?}"),
        }
    }

    #[test]
    fn a_run_started_writes_its_start_id_only_when_it_has_one() {
        let without = stored_run_started(None);
        let start = Uuid::now_v7();
        let with = stored_run_started(Some(("start_id", json!(start))));
        let rewritten_without = JobEvent::decode("RunStarted", without.clone())
            .unwrap()
            .payload()
            .unwrap();
        let rewritten_with = JobEvent::decode("RunStarted", with.clone())
            .unwrap()
            .payload()
            .unwrap();
        assert_eq!(rewritten_without, without);
        assert!(rewritten_without.get("start_id").is_none());
        assert_eq!(rewritten_with, with);
        match JobEvent::decode("RunStarted", with).unwrap() {
            JobEvent::RunStarted(fact) => {
                assert_eq!(fact.start_id, Some(StartId::new(start).unwrap()));
            }
            other => panic!("expected a RunStarted fact, got {other:?}"),
        }
    }

    #[test]
    fn every_fact_addresses_the_job_it_belongs_to() {
        let job = job_id();
        let run_fact = JobEvent::RunCancelled(RunCancelled {
            withdrawn_for: None,
            job_id: job,
            run_id: run_id(),
        });
        let job_fact = JobEvent::JobCompleted(JobCompleted {
            job_id: job,
            resolution_id: resolution_id(),
        });
        assert_eq!(run_fact.job_id(), job);
        assert_eq!(job_fact.job_id(), job);
    }

    #[test]
    fn a_fact_serializes_the_data_a_subscriber_would_otherwise_re_query() {
        let event = JobEvent::RunCompleted(RunCompleted {
            job_id: job_id(),
            run_id: run_id(),
            attempt_number: crate::domain::attempts::AttemptNumber::FIRST,
        });
        let payload = event.payload().unwrap();
        assert_eq!(event.event_type(), "RunCompleted");
        assert_eq!(payload["attempt_number"], 1);
    }

    #[test]
    fn the_stop_request_is_the_only_fact_the_client_never_sees() {
        let internal = JobEvent::RunCancellationRequested(RunCancellationRequested {
            job_id: job_id(),
            run_id: run_id(),
            reason_code: ReasonCode::new("job_cancelled").unwrap(),
            requested_by: None,
            originating_job_id: None,
        });
        let published = JobEvent::RunCancelled(RunCancelled {
            withdrawn_for: None,
            job_id: job_id(),
            run_id: run_id(),
        });
        assert!(!internal.reaches_client());
        assert!(published.reaches_client());
    }

    #[test]
    fn an_affordance_change_is_the_only_fact_that_changes_no_job_state() {
        let flip = JobEvent::JobAffordancesChanged(JobAffordancesChanged {
            job_id: job_id(),
            caused_by_job_id: Some(job_id()),
        });
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
