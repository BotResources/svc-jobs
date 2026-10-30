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

macro_rules! kinds {
    ($($name:ident),+ $(,)?) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub(crate) enum Kind {
            $($name),+
        }

        impl Kind {
            pub(crate) const ALL: &'static [Kind] = &[$(Kind::$name),+];
        }
    };
}

kinds!(
    JobQueued,
    RunDispatched,
    RunStarted,
    RunPlanDeclared,
    RunStepStarted,
    RunCompleted,
    RunFailed,
    RunCancellationRequested,
    RunCancelled,
    RetryScheduled,
    JobCompleted,
    JobFailed,
    JobCancelled,
    ManualRetryStarted,
    JobDeleted,
    JobAffordancesChanged,
    JobPriorityForced,
    JobPriorityRecalculated
);

pub(crate) fn kind_of(event: &JobEvent) -> Kind {
    match event {
        JobEvent::JobQueued(_) => Kind::JobQueued,
        JobEvent::RunDispatched(_) => Kind::RunDispatched,
        JobEvent::RunStarted(_) => Kind::RunStarted,
        JobEvent::RunPlanDeclared(_) => Kind::RunPlanDeclared,
        JobEvent::RunStepStarted(_) => Kind::RunStepStarted,
        JobEvent::RunCompleted(_) => Kind::RunCompleted,
        JobEvent::RunFailed(_) => Kind::RunFailed,
        JobEvent::RunCancellationRequested(_) => Kind::RunCancellationRequested,
        JobEvent::RunCancelled(_) => Kind::RunCancelled,
        JobEvent::RetryScheduled(_) => Kind::RetryScheduled,
        JobEvent::JobCompleted(_) => Kind::JobCompleted,
        JobEvent::JobFailed(_) => Kind::JobFailed,
        JobEvent::JobCancelled(_) => Kind::JobCancelled,
        JobEvent::ManualRetryStarted(_) => Kind::ManualRetryStarted,
        JobEvent::JobDeleted(_) => Kind::JobDeleted,
        JobEvent::JobAffordancesChanged(_) => Kind::JobAffordancesChanged,
        JobEvent::JobPriorityForced(_) => Kind::JobPriorityForced,
        JobEvent::JobPriorityRecalculated(_) => Kind::JobPriorityRecalculated,
    }
}

pub(crate) fn samples_of(kind: Kind) -> Vec<(JobEvent, RunInboxEffect)> {
    let job = job_id();
    let run = run_id();
    let untouched = RunInboxEffect::Untouched;
    match kind {
        Kind::JobQueued => vec![(queued(), RunInboxEffect::JobQueued)],
        Kind::RunDispatched => vec![(
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
        )],
        Kind::RunStarted => vec![(
            JobEvent::RunStarted(RunStarted {
                job_id: job,
                run_id: run,
                runner_type: instance().runner_type().clone(),
                instance_key: instance().instance_key().clone(),
            }),
            RunInboxEffect::RunStarted,
        )],
        Kind::RunPlanDeclared => vec![(
            JobEvent::RunPlanDeclared(RunPlanDeclared {
                job_id: job,
                run_id: run,
                declaration_id: declaration_id(),
                declaration_number: DeclarationNumber::FIRST,
                items: vec![],
            }),
            untouched,
        )],
        Kind::RunStepStarted => vec![(
            JobEvent::RunStepStarted(RunStepStarted {
                job_id: job,
                run_id: run,
                step_index: StepIndex::new(0),
                label: StepLabel::new("read").unwrap(),
                started_at: ts(0),
                closed_step_index: None,
            }),
            untouched,
        )],
        Kind::RunCompleted => vec![(
            JobEvent::RunCompleted(RunCompleted {
                job_id: job,
                run_id: run,
                attempt_number: AttemptNumber::FIRST,
            }),
            RunInboxEffect::RunCompleted { run_id: run },
        )],
        Kind::RunFailed => vec![(
            JobEvent::RunFailed(RunFailed {
                job_id: job,
                run_id: run,
                attempt_number: AttemptNumber::FIRST,
                report: report(RunFailureKind::Permanent),
            }),
            RunInboxEffect::RunFailed,
        )],
        Kind::RunCancellationRequested => vec![(
            JobEvent::RunCancellationRequested(RunCancellationRequested {
                job_id: job,
                run_id: run,
                reason_code: ReasonCode::new("job_cancelled").unwrap(),
                requested_by: None,
                originating_job_id: None,
            }),
            untouched,
        )],
        Kind::RunCancelled => vec![
            (
                cancelled(false),
                RunInboxEffect::RunCancelled { withdrawn: false },
            ),
            (
                cancelled(true),
                RunInboxEffect::RunCancelled { withdrawn: true },
            ),
        ],
        Kind::RetryScheduled => vec![(
            JobEvent::RetryScheduled(RetryScheduled {
                job_id: job,
                schedule_id: schedule_id(),
                failed_run_id: run,
                next_attempt_number: AttemptNumber::new(2).unwrap(),
                due_at: ts(30),
            }),
            RunInboxEffect::RetryScheduled,
        )],
        Kind::JobCompleted => vec![(
            JobEvent::JobCompleted(JobCompleted {
                job_id: job,
                resolution_id: resolution_id(),
            }),
            untouched,
        )],
        Kind::JobFailed => vec![(
            JobEvent::JobFailed(JobFailed {
                job_id: job,
                resolution_id: resolution_id(),
                failure_cause: JobFailureCause::DeclaredByOwner,
                caused_by_run_id: None,
                report: None,
            }),
            RunInboxEffect::JobFailed,
        )],
        Kind::JobCancelled => vec![(
            JobEvent::JobCancelled(JobCancelled {
                job_id: job,
                resolution_id: resolution_id(),
                originating_job_id: None,
            }),
            untouched,
        )],
        Kind::ManualRetryStarted => vec![(
            JobEvent::ManualRetryStarted(ManualRetryStarted {
                job_id: job,
                manual_retry_id: manual_retry_id(),
                failed_resolution_id: resolution_id(),
                successor_job_id: job_id(),
                requested_by: user(),
                run_id: None,
            }),
            untouched,
        )],
        Kind::JobDeleted => vec![(
            JobEvent::JobDeleted(JobDeleted {
                job_id: job,
                deleted_by: user(),
            }),
            untouched,
        )],
        Kind::JobAffordancesChanged => vec![(
            JobEvent::JobAffordancesChanged(JobAffordancesChanged {
                job_id: job,
                caused_by_job_id: None,
            }),
            untouched,
        )],
        Kind::JobPriorityForced => vec![(
            JobEvent::JobPriorityForced(JobPriorityForced {
                job_id: job,
                priority: Priority::HIGHEST,
                forced_by: user(),
                originating_job_id: None,
                requested_priority: None,
            }),
            untouched,
        )],
        Kind::JobPriorityRecalculated => vec![(
            JobEvent::JobPriorityRecalculated(JobPriorityRecalculated {
                job_id: job,
                effective_priority: Priority::HIGHEST,
                previous_priority: Priority::DEFAULT,
                user_id: None,
                requested_priority: None,
            }),
            untouched,
        )],
    }
}

pub(crate) fn every_kind_of_event() -> Vec<(JobEvent, RunInboxEffect)> {
    Kind::ALL
        .iter()
        .flat_map(|kind| samples_of(*kind))
        .collect()
}
