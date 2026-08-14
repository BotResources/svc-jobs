use super::*;
use crate::domain::attempts::AttemptNumber;
use crate::domain::ids::JobId;
use crate::domain::job::JobState;
use crate::domain::job::resolution::{JobFailureCause, JobResolution};
use crate::domain::run::failure::{RunFailureKind, RunFailureReport};
use crate::fixtures::{JobBuilder, RunBuilder, resolution_id, run_id, ts};
use uuid::Uuid;

#[test]
fn a_job_with_no_run_is_pending() {
    // Given: a job just declared by its producer
    let job = JobBuilder::new().build();
    // When/Then: no attempt exists yet, so the job waits
    assert_eq!(job.status(), JobStatus::Pending);
    assert_eq!(job.attempt_count(), 0);
    assert!(job.active_run().is_none());
}

#[test]
fn a_job_with_a_run_and_no_resolution_is_in_progress() {
    // Given: a job whose first attempt is dispatched
    let job = JobBuilder::new()
        .with_run(RunBuilder::new(1).build())
        .build();
    // When/Then: the job reads as in progress with one attempt
    assert_eq!(job.status(), JobStatus::InProgress);
    assert_eq!(job.attempt_count(), 1);
    assert!(job.active_run().is_some());
}

#[test]
fn a_terminal_resolution_governs_the_status_whatever_the_runs_say() {
    // Given: a job whose only run completed but whose owner cancelled it
    let job = JobBuilder::new()
        .with_run(RunBuilder::new(1).started(ts(5)).completed(ts(20)).build())
        .with_resolution(JobResolution::cancelled(resolution_id(), ts(30)))
        .build();
    // When/Then: the resolution is what the job reports
    assert_eq!(job.status(), JobStatus::Cancelled);
    assert!(job.is_terminal());
}

#[test]
fn a_terminal_job_advertises_no_next_attempt() {
    // Given: a failed job whose last run left a retry schedule behind
    let job = JobBuilder::new()
        .with_run(
            RunBuilder::new(1)
                .started(ts(5))
                .failed(ts(20), RunFailureKind::Transient)
                .retry_due(ts(80))
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
        .build();
    // When/Then: a terminal job never schedules another attempt
    assert_eq!(job.next_attempt_at(), None);
}

#[test]
fn the_next_attempt_time_is_the_unconsumed_retry_due_time() {
    // Given: a job whose first attempt failed transiently and was rescheduled
    let job = JobBuilder::new()
        .with_run(
            RunBuilder::new(1)
                .started(ts(5))
                .failed(ts(20), RunFailureKind::Transient)
                .retry_due(ts(80))
                .build(),
        )
        .build();
    // When/Then: the stored due time is what the job advertises
    assert_eq!(job.next_attempt_at(), Some(ts(80)));
}

#[test]
fn a_dispatched_retry_consumes_its_schedule() {
    // Given: a failed first attempt whose retry has already been dispatched
    let failed = RunBuilder::new(1)
        .started(ts(5))
        .failed(ts(20), RunFailureKind::Transient)
        .retry_due(ts(80))
        .build();
    let schedule = failed.retry_schedule().unwrap().id();
    let job = JobBuilder::new()
        .with_run(failed)
        .with_run(RunBuilder::new(2).retry_of_schedule(schedule).build())
        .build();
    // When/Then: no further attempt is pending
    assert_eq!(job.next_attempt_at(), None);
    assert_eq!(job.attempt_count(), 2);
}

#[test]
fn a_job_pointing_at_itself_as_parent_cannot_be_loaded() {
    // Given: a stored job whose parent is itself
    let id = JobId::new(Uuid::now_v7()).unwrap();
    let state = JobState {
        parent_job_id: Some(id),
        ..JobBuilder::new().with_id(id).state()
    };
    // When: it is hydrated
    let result = Job::hydrate(state);
    // Then: the cycle is refused at load
    assert_eq!(
        result.err(),
        Some(JobsError::SelfReference {
            field: "parent_job_id"
        })
    );
}

#[test]
fn a_job_holding_two_live_runs_cannot_be_loaded() {
    // Given: a stored job with two non-terminal attempts
    let state = JobBuilder::new()
        .with_run(RunBuilder::new(1).build())
        .with_run(RunBuilder::new(2).build())
        .state();
    // When: it is hydrated
    let result = Job::hydrate(state);
    // Then: the at-most-one-live-run invariant is enforced at load too
    assert_eq!(
        result.err(),
        Some(JobsError::CorruptState {
            reason_code: "several_non_terminal_runs"
        })
    );
}

#[test]
fn a_resolved_job_still_holding_a_live_run_cannot_be_loaded() {
    // Given: a stored job that reached a terminal resolution while a run stayed open
    let state = JobBuilder::new()
        .with_run(RunBuilder::new(1).started(ts(5)).build())
        .with_resolution(JobResolution::completed(resolution_id(), ts(30)))
        .state();
    // When: it is hydrated
    let result = Job::hydrate(state);
    // Then: a finished job never keeps work in flight — no backstop could reclaim it
    assert_eq!(
        result.err(),
        Some(JobsError::CorruptState {
            reason_code: "terminal_job_with_a_live_run"
        })
    );
}

#[test]
fn a_job_whose_attempts_skip_a_number_cannot_be_loaded() {
    // Given: a stored job whose attempts jump from one to three
    let state = JobBuilder::new()
        .with_run(RunBuilder::new(1).started(ts(5)).completed(ts(10)).build())
        .with_run(RunBuilder::new(3).build())
        .state();
    // When: it is hydrated
    let result = Job::hydrate(state);
    // Then: the gap is refused
    assert_eq!(
        result.err(),
        Some(JobsError::CorruptState {
            reason_code: "attempt_numbers_not_contiguous"
        })
    );
}

fn lowered_to(ceiling: u32) -> ServiceLimits {
    ServiceLimits {
        max_attempts_ceiling: MaxAttempts::new(ceiling).unwrap(),
        ..ServiceLimits::default()
    }
}

#[test]
fn a_job_accepted_above_a_ceiling_since_lowered_still_loads() {
    // Given: a job accepted with five attempts, and an operator who later lowered the ceiling to three
    let state = JobBuilder::new().with_max_attempts(5).state();
    // When: it is read back
    let job = Job::hydrate(state).unwrap();
    // Then: it loads — a service knob never turns accepted history into unreadable data
    assert_eq!(job.max_attempts(), Some(MaxAttempts::new(5).unwrap()));
}

#[test]
fn a_lowered_ceiling_bounds_what_a_job_may_still_spend() {
    // Given: a job holding a stored budget of five under a ceiling since lowered to three
    let job = JobBuilder::new().with_max_attempts(5).build();
    // When: the effective budget is read
    let budget = job.budget(&lowered_to(3));
    // Then: the ceiling bounds the effect, not the load — the fourth attempt is refused
    assert_eq!(budget.get(), 3);
    assert!(!budget.allows(AttemptNumber::new(4).unwrap()));
}

#[test]
fn a_lowered_ceiling_also_bounds_the_service_default() {
    // Given: a job that declared no budget, under a ceiling below the service default
    let job = JobBuilder::new().build();
    // When: the effective budget is read
    // Then: the ceiling governs, never the higher default
    assert_eq!(job.budget(&lowered_to(1)).get(), 1);
}

#[test]
fn a_stored_budget_under_the_ceiling_governs_the_dispatch_bound() {
    // Given: a job that lowered its own budget to two under a ceiling of ten
    let job = JobBuilder::new().with_max_attempts(2).build();
    // When: the effective budget is read
    let budget = job.budget(&ServiceLimits::default());
    // Then: the declared figure is honoured, never widened to the ceiling
    assert_eq!(budget.get(), 2);
    assert!(!budget.allows(AttemptNumber::new(3).unwrap()));
}

#[test]
fn a_job_declaring_no_budget_falls_back_to_the_service_default() {
    // Given: a job that declared no attempt count
    let job = JobBuilder::new().build();
    // When: the effective budget is read
    // Then: the service default governs
    assert_eq!(
        job.budget(&ServiceLimits::default()).get(),
        ServiceLimits::default().default_max_attempts.get()
    );
}

#[test]
fn a_deleted_job_without_a_resolution_cannot_be_loaded() {
    // Given: a stored soft deletion on a job that never reached a terminal state
    let state = JobBuilder::new().deleted(ts(50)).state();
    // When: it is hydrated
    let result = Job::hydrate(state);
    // Then: deletion of live work is refused at load
    assert_eq!(
        result.err(),
        Some(JobsError::CorruptState {
            reason_code: "deleted_job_without_resolution"
        })
    );
}

#[test]
fn a_resolution_blaming_a_run_of_another_job_cannot_be_loaded() {
    // Given: a stored failure attributed to a run this job never dispatched
    let state = JobBuilder::new()
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
                JobFailureCause::TerminalRunFailure,
                Some(run_id()),
            )
            .unwrap(),
        )
        .state();
    // When: it is hydrated
    let result = Job::hydrate(state);
    // Then: the foreign attribution is refused
    assert_eq!(
        result.err(),
        Some(JobsError::CorruptState {
            reason_code: "resolution_names_a_foreign_run"
        })
    );
}

#[test]
fn a_failed_run_report_survives_hydration_for_escalation() {
    // Given: a job whose run failed with a structured report
    let job = JobBuilder::new()
        .with_run(
            RunBuilder::new(1)
                .started(ts(5))
                .failed(ts(20), RunFailureKind::Permanent)
                .build(),
        )
        .build();
    // When: the report is read back
    let report: &RunFailureReport = job
        .latest_run()
        .and_then(Run::terminal)
        .and_then(|terminal| terminal.failure())
        .unwrap();
    // Then: it is intact and permanent
    assert_eq!(report.kind(), RunFailureKind::Permanent);
}
