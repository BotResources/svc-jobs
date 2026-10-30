use super::*;
use crate::domain::run::failure::RunFailureKind;
use crate::fixtures::{JobBuilder, RunBuilder, report, resolution_id, schedule_id, ts};

fn failure(run_id: RunId, kind: RunFailureKind) -> RunFailureFact {
    RunFailureFact {
        run_id,
        report: report(kind),
        retry_schedule_id: schedule_id(),
        resolution_id: resolution_id(),
        jitter: Jitter::MIDPOINT,
        at: ts(20),
    }
}

#[test]
fn a_successful_run_does_not_complete_its_job() {
    let run = RunBuilder::new(1).started(ts(5)).build();
    let run_id = run.id();
    let job = JobBuilder::new().with_run(run).build();
    let result = job
        .record_run_completed(RunCompletedFact { run_id })
        .unwrap();
    assert_eq!(result.events.len(), 1);
    assert!(matches!(
        result.events.first(),
        Some(JobEvent::RunCompleted(_))
    ));
}

#[test]
fn a_transient_failure_with_budget_left_schedules_another_attempt() {
    let run = RunBuilder::new(1).started(ts(5)).build();
    let run_id = run.id();
    let job = JobBuilder::new().with_run(run).build();
    let result = job
        .record_run_failed(
            failure(run_id, RunFailureKind::Transient),
            &RetryPolicy::default(),
            &ServiceLimits::default(),
        )
        .unwrap();
    match result.events.as_slice() {
        [
            JobEvent::RunFailed(failed),
            JobEvent::RetryScheduled(scheduled),
        ] => {
            assert_eq!(failed.attempt_number, AttemptNumber::FIRST);
            assert_eq!(scheduled.failed_run_id, run_id);
            assert_eq!(scheduled.next_attempt_number.get(), 2);
            assert_eq!(scheduled.due_at, ts(30));
        }
        other => panic!("expected a failure and a retry, got {other:?}"),
    }
}

#[test]
fn a_permanent_failure_ends_automatic_retry_immediately() {
    let run = RunBuilder::new(1).started(ts(5)).build();
    let run_id = run.id();
    let job = JobBuilder::new().with_run(run).build();
    let result = job
        .record_run_failed(
            failure(run_id, RunFailureKind::Permanent),
            &RetryPolicy::default(),
            &ServiceLimits::default(),
        )
        .unwrap();
    match result.events.as_slice() {
        [JobEvent::RunFailed(_), JobEvent::JobFailed(job_failed)] => {
            assert_eq!(
                job_failed.failure_cause,
                JobFailureCause::TerminalRunFailure
            );
            assert_eq!(job_failed.caused_by_run_id, Some(run_id));
            assert!(job_failed.report.is_some());
        }
        other => panic!("expected a failure and a job failure, got {other:?}"),
    }
}

#[test]
fn a_transient_failure_on_the_last_attempt_fails_the_job() {
    let run = RunBuilder::new(1).started(ts(5)).build();
    let run_id = run.id();
    let job = JobBuilder::new().with_max_attempts(1).with_run(run).build();
    let result = job
        .record_run_failed(
            failure(run_id, RunFailureKind::Transient),
            &RetryPolicy::default(),
            &ServiceLimits::default(),
        )
        .unwrap();
    assert!(matches!(result.events.last(), Some(JobEvent::JobFailed(_))));
}

#[test]
fn a_runner_retry_hint_lengthens_the_recorded_due_time() {
    let run = RunBuilder::new(1).started(ts(5)).build();
    let run_id = run.id();
    let job = JobBuilder::new().with_run(run).build();
    let hinted = RunFailureFact {
        report: RunFailureReport::new(
            RunFailureKind::Transient,
            crate::domain::keys::ReasonCode::new("provider_rate_limited").unwrap(),
            serde_json::json!({}),
            serde_json::json!({}),
            Some(chrono::TimeDelta::minutes(5)),
        )
        .unwrap(),
        ..failure(run_id, RunFailureKind::Transient)
    };
    let result = job
        .record_run_failed(hinted, &RetryPolicy::default(), &ServiceLimits::default())
        .unwrap();
    match result.events.last() {
        Some(JobEvent::RetryScheduled(scheduled)) => {
            assert_eq!(scheduled.due_at, ts(20) + chrono::TimeDelta::minutes(5));
        }
        other => panic!("expected a retry, got {other:?}"),
    }
}

#[test]
fn a_redelivered_terminal_fact_changes_no_history() {
    let run = RunBuilder::new(1).started(ts(5)).completed(ts(20)).build();
    let run_id = run.id();
    let job = JobBuilder::new().with_run(run).build();
    let result = job
        .record_run_failed(
            failure(run_id, RunFailureKind::Transient),
            &RetryPolicy::default(),
            &ServiceLimits::default(),
        )
        .unwrap();
    assert!(result.is_empty());
}

#[test]
fn a_completion_for_a_run_that_never_started_is_refused() {
    let run = RunBuilder::new(1).build();
    let run_id = run.id();
    let job = JobBuilder::new().with_run(run).build();
    let result = job.record_run_completed(RunCompletedFact { run_id });
    assert_eq!(
        result,
        Err(JobsError::RunNotStarted {
            run_id: run_id.as_uuid()
        })
    );
}

#[test]
fn a_failure_for_a_run_that_never_started_is_refused() {
    let run = RunBuilder::new(1).build();
    let run_id = run.id();
    let job = JobBuilder::new().with_run(run).build();
    let result = job.record_run_failed(
        failure(run_id, RunFailureKind::Transient),
        &RetryPolicy::default(),
        &ServiceLimits::default(),
    );
    assert_eq!(
        result,
        Err(JobsError::RunNotStarted {
            run_id: run_id.as_uuid()
        })
    );
}

#[test]
fn a_success_for_a_live_run_of_a_terminal_job_ends_the_run_and_leaves_the_job() {
    let run = RunBuilder::new(1).started(ts(5)).build();
    let run_id = run.id();
    let job = JobBuilder::new()
        .with_run(run)
        .with_resolution(crate::domain::job::resolution::JobResolution::completed(
            resolution_id(),
            ts(10),
        ))
        .build();
    let result = job
        .record_run_completed(RunCompletedFact { run_id })
        .unwrap();
    assert!(matches!(
        result.events.as_slice(),
        [JobEvent::RunCompleted(_)]
    ));
}

#[test]
fn a_permanent_failure_for_a_live_run_of_a_terminal_job_ends_the_run_and_leaves_the_job() {
    let run = RunBuilder::new(1).started(ts(5)).build();
    let run_id = run.id();
    let job = JobBuilder::new()
        .with_run(run)
        .with_resolution(crate::domain::job::resolution::JobResolution::completed(
            resolution_id(),
            ts(10),
        ))
        .build();
    let result = job
        .record_run_failed(
            failure(run_id, RunFailureKind::Transient),
            &RetryPolicy::default(),
            &ServiceLimits::default(),
        )
        .unwrap();
    assert!(matches!(result.events.as_slice(), [JobEvent::RunFailed(_)]));
}
