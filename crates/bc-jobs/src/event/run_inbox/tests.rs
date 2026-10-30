use super::*;
use crate::commands::job::create::{CreateJob, CreateOutcome, create_job};
use crate::domain::attempts::AttemptNumber;
use crate::domain::job::resolution::JobFailureCause;
use crate::domain::keys::{ReasonCode, StepLabel};
use crate::domain::policy::ServiceLimits;
use crate::domain::priority::Priority;
use crate::domain::requester::Requester;
use crate::domain::run::failure::RunFailureKind;
use crate::domain::run::origin::RunOrigin;
use crate::domain::run::plan::DeclarationNumber;
use crate::domain::run::step::StepIndex;
use crate::event::job_facts::{
    JobAffordancesChanged, JobCancelled, JobCompleted, JobDeleted, JobFailed, JobPriorityForced,
    JobPriorityRecalculated, ManualRetryStarted, RetryScheduled, RunCancellationRequested,
    RunCancelled, RunCompleted, RunDispatched, RunFailed, RunPlanDeclared, RunStarted,
    RunStepStarted,
};
use crate::fixtures::{
    declaration_id, instance, job_id, manual_retry_id, producer, report, resolution_id, run_id,
    runner_type, schedule_id, service_passport, ts, user,
};

fn queued() -> JobEvent {
    let command = CreateJob {
        id: job_id(),
        runner_type: runner_type(),
        producer: producer(),
        config: None,
        parent_job_id: None,
        triggered_by: None,
        source_entity_id: None,
        max_attempts: None,
        requested_priority: Priority::DEFAULT,
        initial_job_id: None,
        requester: Requester::Service(None),
        passport: Some(service_passport()),
        payload: None,
        call: None,
    };
    match create_job(command, None, None, None, None, &ServiceLimits::default()) {
        Ok(CreateOutcome::Queued(result)) => result.events.into_iter().next().unwrap(),
        other => panic!("a fresh declaration is queued, got {other:?}"),
    }
}

fn cancelled(withdrawn: bool) -> JobEvent {
    JobEvent::RunCancelled(RunCancelled {
        job_id: job_id(),
        run_id: run_id(),
        withdrawn_for: withdrawn.then(|| ReasonCode::new("job_cancelled").unwrap()),
    })
}

pub(crate) fn every_kind_of_event() -> Vec<(JobEvent, RunInboxEffect)> {
    let job = job_id();
    let run = run_id();
    let untouched = RunInboxEffect::Untouched;
    vec![
        (queued(), RunInboxEffect::JobQueued),
        (
            JobEvent::RunDispatched(RunDispatched {
                job_id: job,
                run_id: run,
                runner_type: runner_type(),
                attempt_number: AttemptNumber::FIRST,
                origin: RunOrigin::Initial,
                automatic_retry_schedule_id: None,
                automatic_retry_of_run_id: None,
                max_run_duration: None,
                claimed_by: None,
            }),
            untouched,
        ),
        (
            JobEvent::RunStarted(RunStarted {
                job_id: job,
                run_id: run,
                runner_type: instance().runner_type().clone(),
                instance_key: instance().instance_key().clone(),
            }),
            RunInboxEffect::RunStarted,
        ),
        (
            JobEvent::RunPlanDeclared(RunPlanDeclared {
                job_id: job,
                run_id: run,
                declaration_id: declaration_id(),
                declaration_number: DeclarationNumber::FIRST,
                items: vec![],
            }),
            untouched,
        ),
        (
            JobEvent::RunStepStarted(RunStepStarted {
                job_id: job,
                run_id: run,
                step_index: StepIndex::new(0),
                label: StepLabel::new("read").unwrap(),
                started_at: ts(0),
                closed_step_index: None,
            }),
            untouched,
        ),
        (
            JobEvent::RunCompleted(RunCompleted {
                job_id: job,
                run_id: run,
                attempt_number: AttemptNumber::FIRST,
            }),
            RunInboxEffect::RunCompleted { run_id: run },
        ),
        (
            JobEvent::RunFailed(RunFailed {
                job_id: job,
                run_id: run,
                attempt_number: AttemptNumber::FIRST,
                report: report(RunFailureKind::Permanent),
            }),
            RunInboxEffect::RunFailed,
        ),
        (
            JobEvent::RunCancellationRequested(RunCancellationRequested {
                job_id: job,
                run_id: run,
                reason_code: ReasonCode::new("job_cancelled").unwrap(),
                requested_by: None,
                originating_job_id: None,
            }),
            untouched,
        ),
        (
            cancelled(false),
            RunInboxEffect::RunCancelled { withdrawn: false },
        ),
        (
            cancelled(true),
            RunInboxEffect::RunCancelled { withdrawn: true },
        ),
        (
            JobEvent::RetryScheduled(RetryScheduled {
                job_id: job,
                schedule_id: schedule_id(),
                failed_run_id: run,
                next_attempt_number: AttemptNumber::new(2).unwrap(),
                due_at: ts(30),
            }),
            RunInboxEffect::RetryScheduled,
        ),
        (
            JobEvent::JobCompleted(JobCompleted {
                job_id: job,
                resolution_id: resolution_id(),
            }),
            untouched,
        ),
        (
            JobEvent::JobFailed(JobFailed {
                job_id: job,
                resolution_id: resolution_id(),
                failure_cause: JobFailureCause::DeclaredByOwner,
                caused_by_run_id: None,
                report: None,
            }),
            RunInboxEffect::JobFailed,
        ),
        (
            JobEvent::JobCancelled(JobCancelled {
                job_id: job,
                resolution_id: resolution_id(),
                originating_job_id: None,
            }),
            untouched,
        ),
        (
            JobEvent::ManualRetryStarted(ManualRetryStarted {
                job_id: job,
                manual_retry_id: manual_retry_id(),
                failed_resolution_id: resolution_id(),
                successor_job_id: job_id(),
                requested_by: user(),
                run_id: None,
            }),
            untouched,
        ),
        (
            JobEvent::JobDeleted(JobDeleted {
                job_id: job,
                deleted_by: user(),
            }),
            untouched,
        ),
        (
            JobEvent::JobAffordancesChanged(JobAffordancesChanged {
                job_id: job,
                caused_by_job_id: None,
            }),
            untouched,
        ),
        (
            JobEvent::JobPriorityForced(JobPriorityForced {
                job_id: job,
                priority: Priority::HIGHEST,
                forced_by: user(),
                originating_job_id: None,
                requested_priority: None,
            }),
            untouched,
        ),
        (
            JobEvent::JobPriorityRecalculated(JobPriorityRecalculated {
                job_id: job,
                effective_priority: Priority::HIGHEST,
                previous_priority: Priority::DEFAULT,
                user_id: None,
                requested_priority: None,
            }),
            untouched,
        ),
    ]
}

#[test]
fn every_kind_of_event_is_classified_by_what_it_does_to_the_inbox_of_a_run() {
    for (event, expected) in every_kind_of_event() {
        assert_eq!(event.run_inbox_effect(), expected, "{}", event.event_type());
    }
}

#[test]
fn the_sample_holds_every_kind_of_event_the_aggregate_records() {
    let mut kinds: Vec<&str> = every_kind_of_event()
        .iter()
        .map(|(event, _)| event.event_type())
        .collect();
    kinds.sort_unstable();
    kinds.dedup();
    assert_eq!(kinds.len(), 18);
}

#[test]
fn only_the_events_that_start_or_end_a_run_or_write_a_notice_serialize_with_the_directory() {
    let serializing: Vec<&str> = every_kind_of_event()
        .iter()
        .filter(|(event, _)| {
            event
                .run_inbox_effect()
                .serializes_with_the_tree_directory()
        })
        .map(|(event, _)| event.event_type())
        .collect();
    assert_eq!(
        serializing,
        [
            "JobQueued",
            "RunStarted",
            "RunCompleted",
            "RunFailed",
            "RunCancelled",
            "RunCancelled",
            "RetryScheduled",
            "JobFailed"
        ]
    );
}
