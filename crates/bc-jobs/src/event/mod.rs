pub mod fleet;
pub mod job;
pub mod job_facts;

use br_core_events::{DomainEvent, EventMetadata};
use chrono::{DateTime, Utc};

use crate::domain::ids::EventId;
use crate::error::JobsError;
use crate::event::fleet::{FleetEvent, RUNNER_TYPE_AGGREGATE_TYPE};
use crate::event::job::{JOB_AGGREGATE_TYPE, JobEvent};

#[derive(Debug, Clone)]
pub struct Recorded<E> {
    pub id: EventId,
    pub event: E,
    pub occurred_at: DateTime<Utc>,
}

impl Recorded<JobEvent> {
    pub fn into_envelope(self, metadata: EventMetadata) -> Result<DomainEvent, JobsError> {
        Ok(DomainEvent::new(
            self.id.as_uuid(),
            self.event.job_id().as_uuid(),
            JOB_AGGREGATE_TYPE,
            self.event.event_type(),
            self.event.payload()?,
            metadata,
            self.occurred_at,
        ))
    }
}

impl Recorded<FleetEvent> {
    pub fn into_envelope(self, metadata: EventMetadata) -> Result<DomainEvent, JobsError> {
        Ok(DomainEvent::new(
            self.id.as_uuid(),
            self.event.runner_type_id().as_uuid(),
            RUNNER_TYPE_AGGREGATE_TYPE,
            self.event.event_type(),
            self.event.payload()?,
            metadata,
            self.occurred_at,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::job_facts::JobCompleted;
    use crate::fixtures::{job_id, resolution_id, ts};
    use br_core_events::{Actor, UserId};
    use uuid::Uuid;

    #[test]
    fn a_job_fact_is_stored_under_its_aggregate_root() {
        // Given: a completion fact and the metadata of the actor that caused it
        let job = job_id();
        let recorded = Recorded {
            id: EventId::new(Uuid::now_v7()).unwrap(),
            event: JobEvent::JobCompleted(JobCompleted {
                job_id: job,
                resolution_id: resolution_id(),
            }),
            occurred_at: ts(30),
        };
        let metadata = EventMetadata::new(Actor::Human(UserId(Uuid::now_v7())), Uuid::now_v7());
        // When: it is wrapped in the shared envelope
        let envelope = recorded.into_envelope(metadata).unwrap();
        // Then: it is addressed to the Job aggregate, carrying its own payload
        assert_eq!(envelope.aggregate_type, "Job");
        assert_eq!(envelope.aggregate_id, job.as_uuid());
        assert_eq!(envelope.event_type, "JobCompleted");
        assert_eq!(envelope.occurred_at, ts(30));
    }
}
