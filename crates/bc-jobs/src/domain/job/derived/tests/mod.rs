use super::*;
use crate::domain::attempts::AttemptNumber;
use crate::domain::ids::JobId;
use crate::domain::job::JobState;
use crate::domain::job::resolution::{JobFailureCause, JobResolution};
use crate::domain::run::failure::{RunFailureKind, RunFailureReport};
use crate::fixtures::{JobBuilder, RunBuilder, resolution_id, run_id, ts};
use uuid::Uuid;

mod budget;
mod progression;

#[test]
fn a_job_with_no_run_is_pending() {
    let job = JobBuilder::new().build();
    assert_eq!(job.status(), JobStatus::Pending);
    assert_eq!(job.attempt_count(), 0);
    assert!(job.active_run().is_none());
}

#[test]
fn a_job_with_a_run_and_no_resolution_is_in_progress() {
    let job = JobBuilder::new()
        .with_run(RunBuilder::new(1).build())
        .build();
    assert_eq!(job.status(), JobStatus::InProgress);
    assert_eq!(job.attempt_count(), 1);
    assert!(job.active_run().is_some());
}

#[test]
fn a_terminal_resolution_governs_the_status_whatever_the_runs_say() {
    let job = JobBuilder::new()
        .with_run(RunBuilder::new(1).started(ts(5)).completed(ts(20)).build())
        .with_resolution(JobResolution::cancelled(resolution_id(), ts(30)))
        .build();
    assert_eq!(job.status(), JobStatus::Cancelled);
    assert!(job.is_terminal());
}

#[test]
fn a_terminal_job_advertises_no_next_attempt() {
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
    assert_eq!(job.next_attempt_at(), None);
}

#[test]
fn the_next_attempt_time_is_the_unconsumed_retry_due_time() {
    let job = JobBuilder::new()
        .with_run(
            RunBuilder::new(1)
                .started(ts(5))
                .failed(ts(20), RunFailureKind::Transient)
                .retry_due(ts(80))
                .build(),
        )
        .build();
    assert_eq!(job.next_attempt_at(), Some(ts(80)));
}

#[test]
fn a_dispatched_retry_consumes_its_schedule() {
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
    assert_eq!(job.next_attempt_at(), None);
    assert_eq!(job.attempt_count(), 2);
}

#[test]
fn a_job_pointing_at_itself_as_parent_cannot_be_loaded() {
    let id = JobId::new(Uuid::now_v7()).unwrap();
    let state = JobState {
        parent_job_id: Some(id),
        ..JobBuilder::new().with_id(id).state()
    };
    let result = Job::hydrate(state);
    assert_eq!(
        result.err(),
        Some(JobsError::SelfReference {
            field: "parent_job_id"
        })
    );
}

#[test]
fn a_job_holding_two_live_runs_cannot_be_loaded() {
    let state = JobBuilder::new()
        .with_run(RunBuilder::new(1).build())
        .with_run(RunBuilder::new(2).build())
        .state();
    let result = Job::hydrate(state);
    assert_eq!(
        result.err(),
        Some(JobsError::CorruptState {
            reason_code: "several_non_terminal_runs"
        })
    );
}

#[test]
fn a_resolved_job_may_still_hold_a_live_run_that_ends_by_its_own_fact() {
    let state = JobBuilder::new()
        .with_run(RunBuilder::new(1).started(ts(5)).build())
        .with_resolution(JobResolution::completed(resolution_id(), ts(30)))
        .state();
    let job = Job::hydrate(state).expect("a resolved job with a live run loads");
    assert!(job.is_terminal());
    assert!(job.active_run().is_some());
}

#[test]
fn a_job_whose_attempts_skip_a_number_cannot_be_loaded() {
    let state = JobBuilder::new()
        .with_run(RunBuilder::new(1).started(ts(5)).completed(ts(10)).build())
        .with_run(RunBuilder::new(3).build())
        .state();
    let result = Job::hydrate(state);
    assert_eq!(
        result.err(),
        Some(JobsError::CorruptState {
            reason_code: "attempt_numbers_not_contiguous"
        })
    );
}

#[test]
fn a_deleted_job_without_a_resolution_cannot_be_loaded() {
    let state = JobBuilder::new().deleted(ts(50)).state();
    let result = Job::hydrate(state);
    assert_eq!(
        result.err(),
        Some(JobsError::CorruptState {
            reason_code: "deleted_job_without_resolution"
        })
    );
}

#[test]
fn a_resolution_blaming_a_run_of_another_job_cannot_be_loaded() {
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
    let result = Job::hydrate(state);
    assert_eq!(
        result.err(),
        Some(JobsError::CorruptState {
            reason_code: "resolution_names_a_foreign_run"
        })
    );
}

#[test]
fn a_failed_run_report_survives_hydration_for_escalation() {
    let job = JobBuilder::new()
        .with_run(
            RunBuilder::new(1)
                .started(ts(5))
                .failed(ts(20), RunFailureKind::Permanent)
                .build(),
        )
        .build();
    let report: &RunFailureReport = job
        .latest_run()
        .and_then(Run::terminal)
        .and_then(|terminal| terminal.failure())
        .unwrap();
    assert_eq!(report.kind(), RunFailureKind::Permanent);
}
