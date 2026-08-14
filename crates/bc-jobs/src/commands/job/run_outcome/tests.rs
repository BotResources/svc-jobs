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
    // Given: a job whose only attempt is running
    let run = RunBuilder::new(1).started(ts(5)).build();
    let run_id = run.id();
    let job = JobBuilder::new().with_run(run).build();
    // When: the runner reports success
    let result = job
        .record_run_completed(RunCompletedFact { run_id })
        .unwrap();
    // Then: only the run completes — the owner alone finishes the job
    assert_eq!(result.events.len(), 1);
    assert!(matches!(
        result.events.first(),
        Some(JobEvent::RunCompleted(_))
    ));
}

#[test]
fn a_transient_failure_with_budget_left_schedules_another_attempt() {
    // Given: a running first attempt on a job allowed three attempts
    let run = RunBuilder::new(1).started(ts(5)).build();
    let run_id = run.id();
    let job = JobBuilder::new().with_run(run).build();
    // When: it fails transiently
    let result = job
        .record_run_failed(
            failure(run_id, RunFailureKind::Transient),
            &RetryPolicy::default(),
            &ServiceLimits::default(),
        )
        .unwrap();
    // Then: the failure is recorded and the next attempt gets a stored due time
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
    // Given: a running first attempt with budget to spare
    let run = RunBuilder::new(1).started(ts(5)).build();
    let run_id = run.id();
    let job = JobBuilder::new().with_run(run).build();
    // When: it fails permanently
    let result = job
        .record_run_failed(
            failure(run_id, RunFailureKind::Permanent),
            &RetryPolicy::default(),
            &ServiceLimits::default(),
        )
        .unwrap();
    // Then: the job fails at once, carrying the report to its owner
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
    // Given: a job allowed one attempt, currently running it
    let run = RunBuilder::new(1).started(ts(5)).build();
    let run_id = run.id();
    let job = JobBuilder::new().with_max_attempts(1).with_run(run).build();
    // When: the attempt fails transiently
    let result = job
        .record_run_failed(
            failure(run_id, RunFailureKind::Transient),
            &RetryPolicy::default(),
            &ServiceLimits::default(),
        )
        .unwrap();
    // Then: the budget is spent, so the failure becomes terminal
    assert!(matches!(result.events.last(), Some(JobEvent::JobFailed(_))));
}

#[test]
fn a_runner_retry_hint_lengthens_the_recorded_due_time() {
    // Given: a run failing transiently with a five minute hint
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
    // When: the failure is recorded
    let result = job
        .record_run_failed(hinted, &RetryPolicy::default(), &ServiceLimits::default())
        .unwrap();
    // Then: the hint wins over the shorter platform backoff
    match result.events.last() {
        Some(JobEvent::RetryScheduled(scheduled)) => {
            assert_eq!(scheduled.due_at, ts(20) + chrono::TimeDelta::minutes(5));
        }
        other => panic!("expected a retry, got {other:?}"),
    }
}

#[test]
fn a_redelivered_terminal_fact_changes_no_history() {
    // Given: a run that already completed
    let run = RunBuilder::new(1).started(ts(5)).completed(ts(20)).build();
    let run_id = run.id();
    let job = JobBuilder::new().with_run(run).build();
    // When: a contradicting failure arrives for the same run
    let result = job
        .record_run_failed(
            failure(run_id, RunFailureKind::Transient),
            &RetryPolicy::default(),
            &ServiceLimits::default(),
        )
        .unwrap();
    // Then: the run never reopens — the fact is acknowledged and discarded
    assert!(result.is_empty());
}

#[test]
fn a_completion_for_a_run_that_never_started_is_refused() {
    // Given: a dispatched run whose start fact has not been processed yet
    let run = RunBuilder::new(1).build();
    let run_id = run.id();
    let job = JobBuilder::new().with_run(run).build();
    // When: the completion arrives first, the status subjects being distinct and at-least-once
    let result = job.record_run_completed(RunCompletedFact { run_id });
    // Then: the fact is refused for redelivery rather than settling a run with no instance
    assert_eq!(
        result,
        Err(JobsError::RunNotStarted {
            run_id: run_id.as_uuid()
        })
    );
}

#[test]
fn a_failure_for_a_run_that_never_started_is_refused() {
    // Given: a dispatched run whose start fact has not been processed yet
    let run = RunBuilder::new(1).build();
    let run_id = run.id();
    let job = JobBuilder::new().with_run(run).build();
    // When: a failure arrives before the start
    let result = job.record_run_failed(
        failure(run_id, RunFailureKind::Transient),
        &RetryPolicy::default(),
        &ServiceLimits::default(),
    );
    // Then: no attempt is burned and no retry is scheduled on a run nobody ever claimed
    assert_eq!(
        result,
        Err(JobsError::RunNotStarted {
            run_id: run_id.as_uuid()
        })
    );
}
