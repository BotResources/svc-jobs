use super::*;
use crate::fixtures::{JobBuilder, RunBuilder, resolution_id, schedule_id, ts};
use chrono::TimeDelta;

fn reclaim(run_id: RunId, at: DateTime<Utc>) -> ReclaimRun {
    ReclaimRun {
        run_id,
        retry_schedule_id: schedule_id(),
        resolution_id: resolution_id(),
        jitter: Jitter::MIDPOINT,
        at,
    }
}

#[test]
fn losing_an_instance_fails_its_run_transiently_so_retry_policy_applies() {
    // Given: a job whose run is executing on an instance that disappears
    let run = RunBuilder::new(1).started(ts(5)).build();
    let run_id = run.id();
    let job = JobBuilder::new().with_run(run).build();
    // When: the loss is observed
    let result = job
        .fail_run_for_instance_loss(
            reclaim(run_id, ts(50)),
            &RetryPolicy::default(),
            &ServiceLimits::default(),
        )
        .unwrap();
    // Then: the run fails with the instance_lost reason and another attempt is scheduled
    match result.events.as_slice() {
        [JobEvent::RunFailed(failed), JobEvent::RetryScheduled(_)] => {
            assert_eq!(failed.report.reason_code().as_str(), "instance_lost");
            assert_eq!(failed.report.kind(), RunFailureKind::Transient);
        }
        other => panic!("expected a transient failure and a retry, got {other:?}"),
    }
}

#[test]
fn a_run_past_the_maximum_duration_is_stopped_and_failed_as_transient() {
    // Given: a run started long before the maximum duration ceiling
    let run = RunBuilder::new(1).started(ts(0)).build();
    let run_id = run.id();
    let job = JobBuilder::new().with_run(run).build();
    let past_the_ceiling = ts(0) + TimeDelta::hours(80);
    // When: the backstop reclaims it
    let result = job
        .fail_run_for_max_duration(
            reclaim(run_id, past_the_ceiling),
            &RetryPolicy::default(),
            &ServiceLimits::default(),
        )
        .unwrap();
    // Then: cancellation is requested first, then the transient failure and its retry
    match result.events.as_slice() {
        [
            JobEvent::RunCancellationRequested(requested),
            JobEvent::RunFailed(failed),
            JobEvent::RetryScheduled(_),
        ] => {
            assert_eq!(requested.reason_code.as_str(), "run_timeout");
            assert_eq!(failed.report.kind(), RunFailureKind::Transient);
        }
        other => panic!("expected a stop request, a failure and a retry, got {other:?}"),
    }
}

#[test]
fn a_run_still_within_the_maximum_duration_is_left_alone() {
    // Given: a run started a minute ago
    let run = RunBuilder::new(1).started(ts(0)).build();
    let run_id = run.id();
    let job = JobBuilder::new().with_run(run).build();
    // When: the backstop looks at it
    let result = job.fail_run_for_max_duration(
        reclaim(run_id, ts(60)),
        &RetryPolicy::default(),
        &ServiceLimits::default(),
    );
    // Then: it refuses to reclaim work that is still within its budget
    assert_eq!(
        result,
        Err(JobsError::RunWithinMaxDuration {
            run_id: run_id.as_uuid()
        })
    );
}

#[test]
fn a_job_abandoned_by_its_owner_fails_with_the_inactivity_cause() {
    // Given: a job whose only run completed and whose owner never finished it
    let job = JobBuilder::new()
        .with_run(RunBuilder::new(1).started(ts(5)).completed(ts(20)).build())
        .build();
    let long_after = ts(20) + TimeDelta::hours(30);
    // When: the inactivity backstop runs
    let result = job
        .fail_for_inactivity(
            FailForInactivity {
                resolution_id: resolution_id(),
                at: long_after,
            },
            &ServiceLimits::default(),
        )
        .unwrap();
    // Then: the job fails with the inactivity cause and no run to blame
    match result.events.as_slice() {
        [JobEvent::JobFailed(fact)] => {
            assert_eq!(fact.failure_cause, JobFailureCause::InactivityTimeout);
            assert_eq!(fact.caused_by_run_id, None);
        }
        other => panic!("expected a JobFailed fact, got {other:?}"),
    }
}

#[test]
fn a_job_with_a_live_run_is_never_reclaimed_for_inactivity() {
    // Given: a job whose run is still executing, however long it has been
    let job = JobBuilder::new()
        .with_run(RunBuilder::new(1).started(ts(5)).build())
        .build();
    // When: the backstop runs much later
    let result = job.fail_for_inactivity(
        FailForInactivity {
            resolution_id: resolution_id(),
            at: ts(5) + TimeDelta::hours(30),
        },
        &ServiceLimits::default(),
    );
    // Then: live work is never reclaimed by the inactivity backstop
    assert_eq!(result, Err(JobsError::JobStillActive));
}

#[test]
fn a_job_with_a_scheduled_retry_is_never_reclaimed_for_inactivity() {
    // Given: a job waiting on a scheduled retry
    let job = JobBuilder::new()
        .with_run(
            RunBuilder::new(1)
                .started(ts(5))
                .failed(ts(20), RunFailureKind::Transient)
                .retry_due(ts(80))
                .build(),
        )
        .build();
    // When: the backstop runs after the inactivity window
    let result = job.fail_for_inactivity(
        FailForInactivity {
            resolution_id: resolution_id(),
            at: ts(80) + TimeDelta::hours(30),
        },
        &ServiceLimits::default(),
    );
    // Then: scheduled work is not abandoned work
    assert_eq!(result, Err(JobsError::JobStillActive));
}

#[test]
fn a_job_idle_for_less_than_the_window_is_left_alone() {
    // Given: a job idle for one hour with a twenty-four hour window
    let job = JobBuilder::new()
        .with_run(RunBuilder::new(1).started(ts(5)).completed(ts(20)).build())
        .build();
    // When: the backstop runs too early
    let result = job.fail_for_inactivity(
        FailForInactivity {
            resolution_id: resolution_id(),
            at: ts(20) + TimeDelta::hours(1),
        },
        &ServiceLimits::default(),
    );
    // Then: it refuses, naming the moment the job went idle
    assert_eq!(
        result,
        Err(JobsError::InactivityTimeoutNotReached { idle_since: ts(20) })
    );
}

#[test]
fn a_pending_job_is_not_reclaimed_for_inactivity() {
    // Given: a job that never got a run because its runner type stayed unavailable
    let job = JobBuilder::new().build();
    // When: the backstop runs long after
    let result = job.fail_for_inactivity(
        FailForInactivity {
            resolution_id: resolution_id(),
            at: ts(0) + TimeDelta::hours(300),
        },
        &ServiceLimits::default(),
    );
    // Then: waiting for a fleet is not inactivity
    assert_eq!(
        result,
        Err(JobsError::JobNotInProgress { status: "PENDING" })
    );
}
