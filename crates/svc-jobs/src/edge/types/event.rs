use async_graphql::{Interface, SimpleObject};
use bc_jobs::event::job::JobEvent;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use super::enums::{GqlJobFailureCause, GqlRunFailureKind};

macro_rules! job_event {
    ($name:ident, $wire:literal, { $($field:ident : $type:ty),* $(,)? }) => {
        #[derive(SimpleObject, Clone)]
        #[graphql(name = $wire)]
        pub struct $name {
            pub id: Uuid,
            pub job_id: Uuid,
            pub occurred_at: DateTime<Utc>,
            $(pub $field: $type,)*
        }
    };
}

job_event!(GqlJobQueuedEvent, "JobsJobQueuedEvent", { parent_job_id: Option<Uuid> });
job_event!(GqlRunDispatchedEvent, "JobsRunDispatchedEvent", { run_id: Uuid, attempt_number: i32 });
job_event!(GqlRunStartedEvent, "JobsRunStartedEvent", { run_id: Uuid, instance_key: String });
job_event!(GqlRunPlanDeclaredEvent, "JobsRunPlanDeclaredEvent", { run_id: Uuid, declaration_number: i32 });
job_event!(GqlRunStepStartedEvent, "JobsRunStepStartedEvent", { run_id: Uuid, step_index: i32 });
job_event!(GqlRunCompletedEvent, "JobsRunCompletedEvent", { run_id: Uuid });
job_event!(GqlRunFailedEvent, "JobsRunFailedEvent", { run_id: Uuid, failure_kind: GqlRunFailureKind, reason_code: String });
job_event!(GqlRunCancelledEvent, "JobsRunCancelledEvent", { run_id: Uuid });
job_event!(GqlRetryScheduledEvent, "JobsRetryScheduledEvent", { failed_run_id: Uuid, due_at: DateTime<Utc> });
job_event!(GqlJobCompletedEvent, "JobsJobCompletedEvent", { resolution_id: Uuid });
job_event!(GqlJobFailedEvent, "JobsJobFailedEvent", { resolution_id: Uuid, failure_cause: GqlJobFailureCause });
job_event!(GqlJobCancelledEvent, "JobsJobCancelledEvent", { resolution_id: Uuid });
job_event!(GqlManualRetryStartedEvent, "JobsManualRetryStartedEvent", { successor_job_id: Uuid, manual_retry_id: Uuid, run_id: Uuid });
job_event!(GqlJobDeletedEvent, "JobsJobDeletedEvent", { deleted_by_id: Uuid });
job_event!(GqlJobAffordancesChangedEvent, "JobsJobAffordancesChangedEvent", { caused_by_job_id: Option<Uuid> });

#[derive(Interface, Clone)]
// clippy reads the repeated `field(...)` of an async-graphql interface as one duplicated attribute
#[allow(clippy::duplicated_attributes)]
#[graphql(
    name = "JobsJobEvent",
    field(name = "id", ty = "&Uuid"),
    field(name = "job_id", ty = "&Uuid"),
    field(name = "occurred_at", ty = "&DateTime<Utc>")
)]
pub enum GqlJobEvent {
    Queued(GqlJobQueuedEvent),
    RunDispatched(GqlRunDispatchedEvent),
    RunStarted(GqlRunStartedEvent),
    RunPlanDeclared(GqlRunPlanDeclaredEvent),
    RunStepStarted(GqlRunStepStartedEvent),
    RunCompleted(GqlRunCompletedEvent),
    RunFailed(GqlRunFailedEvent),
    RunCancelled(GqlRunCancelledEvent),
    RetryScheduled(GqlRetryScheduledEvent),
    JobCompleted(GqlJobCompletedEvent),
    JobFailed(GqlJobFailedEvent),
    JobCancelled(GqlJobCancelledEvent),
    ManualRetryStarted(GqlManualRetryStartedEvent),
    JobDeleted(GqlJobDeletedEvent),
    AffordancesChanged(GqlJobAffordancesChangedEvent),
}

pub fn of_domain(id: Uuid, occurred_at: DateTime<Utc>, event: &JobEvent) -> Option<GqlJobEvent> {
    if !event.reaches_client() {
        return None;
    }
    let job_id = event.job_id().as_uuid();
    let carried = match event {
        JobEvent::JobQueued(fact) => GqlJobEvent::Queued(GqlJobQueuedEvent {
            id,
            job_id,
            occurred_at,
            parent_job_id: fact.parent_job_id.map(|id| id.as_uuid()),
        }),
        JobEvent::RunDispatched(fact) => GqlJobEvent::RunDispatched(GqlRunDispatchedEvent {
            id,
            job_id,
            occurred_at,
            run_id: fact.run_id.as_uuid(),
            attempt_number: i32::try_from(fact.attempt_number.get()).unwrap_or(i32::MAX),
        }),
        JobEvent::RunStarted(fact) => GqlJobEvent::RunStarted(GqlRunStartedEvent {
            id,
            job_id,
            occurred_at,
            run_id: fact.run_id.as_uuid(),
            instance_key: fact.instance_key.as_str().to_owned(),
        }),
        JobEvent::RunPlanDeclared(fact) => GqlJobEvent::RunPlanDeclared(GqlRunPlanDeclaredEvent {
            id,
            job_id,
            occurred_at,
            run_id: fact.run_id.as_uuid(),
            declaration_number: i32::try_from(fact.declaration_number.get()).unwrap_or(i32::MAX),
        }),
        JobEvent::RunStepStarted(fact) => GqlJobEvent::RunStepStarted(GqlRunStepStartedEvent {
            id,
            job_id,
            occurred_at,
            run_id: fact.run_id.as_uuid(),
            step_index: i32::try_from(fact.step_index.get()).unwrap_or(i32::MAX),
        }),
        JobEvent::RunCompleted(fact) => GqlJobEvent::RunCompleted(GqlRunCompletedEvent {
            id,
            job_id,
            occurred_at,
            run_id: fact.run_id.as_uuid(),
        }),
        JobEvent::RunFailed(fact) => GqlJobEvent::RunFailed(GqlRunFailedEvent {
            id,
            job_id,
            occurred_at,
            run_id: fact.run_id.as_uuid(),
            failure_kind: fact.report.kind().into(),
            reason_code: fact.report.reason_code().as_str().to_owned(),
        }),
        JobEvent::RunCancelled(fact) => GqlJobEvent::RunCancelled(GqlRunCancelledEvent {
            id,
            job_id,
            occurred_at,
            run_id: fact.run_id.as_uuid(),
        }),
        JobEvent::RetryScheduled(fact) => GqlJobEvent::RetryScheduled(GqlRetryScheduledEvent {
            id,
            job_id,
            occurred_at,
            failed_run_id: fact.failed_run_id.as_uuid(),
            due_at: fact.due_at,
        }),
        JobEvent::JobCompleted(fact) => GqlJobEvent::JobCompleted(GqlJobCompletedEvent {
            id,
            job_id,
            occurred_at,
            resolution_id: fact.resolution_id.as_uuid(),
        }),
        JobEvent::JobFailed(fact) => GqlJobEvent::JobFailed(GqlJobFailedEvent {
            id,
            job_id,
            occurred_at,
            resolution_id: fact.resolution_id.as_uuid(),
            failure_cause: fact.failure_cause.into(),
        }),
        JobEvent::JobCancelled(fact) => GqlJobEvent::JobCancelled(GqlJobCancelledEvent {
            id,
            job_id,
            occurred_at,
            resolution_id: fact.resolution_id.as_uuid(),
        }),
        JobEvent::ManualRetryStarted(fact) => {
            GqlJobEvent::ManualRetryStarted(GqlManualRetryStartedEvent {
                id,
                job_id,
                occurred_at,
                successor_job_id: fact.successor_job_id.as_uuid(),
                manual_retry_id: fact.manual_retry_id.as_uuid(),
                run_id: fact.run_id.as_uuid(),
            })
        }
        JobEvent::JobDeleted(fact) => GqlJobEvent::JobDeleted(GqlJobDeletedEvent {
            id,
            job_id,
            occurred_at,
            deleted_by_id: fact.deleted_by.id().0,
        }),
        JobEvent::JobAffordancesChanged(fact) => {
            GqlJobEvent::AffordancesChanged(GqlJobAffordancesChangedEvent {
                id,
                job_id,
                occurred_at,
                caused_by_job_id: fact.caused_by_job_id.map(|id| id.as_uuid()),
            })
        }
        JobEvent::RunCancellationRequested(_) => return None,
    };
    Some(carried)
}
