use super::*;
use crate::domain::fleet::dispatch_mode::DispatchMode;
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

#[test]
fn a_reservation_never_started_fails_transiently_with_claim_lost_and_the_job_waits_again() {
    // Given: a run reserved by a claim at five seconds and never started
    let run = RunBuilder::new(1).reserved(ts(5)).build();
    let run_id = run.id();
    let job = JobBuilder::new().with_run(run).build();
    // When: the backstop sweeps past the sixty second reservation delay
    let result = job
        .fail_run_for_lost_claim(
            reclaim(run_id, ts(70)),
            TimeDelta::seconds(60),
            &RetryPolicy::default(),
            &ServiceLimits::default(),
        )
        .unwrap();
    // Then: the instance is told to stop, the run fails as transient with claim_lost, and a
    // retry is scheduled so the job waits again
    match result.events.as_slice() {
        [
            JobEvent::RunCancellationRequested(requested),
            JobEvent::RunFailed(failed),
            JobEvent::RetryScheduled(_),
        ] => {
            assert_eq!(requested.reason_code.as_str(), "claim_lost");
            assert_eq!(failed.report.reason_code().as_str(), "claim_lost");
            assert_eq!(failed.report.kind(), RunFailureKind::Transient);
        }
        other => panic!("expected a stop, a transient failure and a retry, got {other:?}"),
    }
}

#[test]
fn a_reservation_within_its_delay_is_left_alone() {
    // Given: a run reserved at five seconds
    let run = RunBuilder::new(1).reserved(ts(5)).build();
    let run_id = run.id();
    let job = JobBuilder::new().with_run(run).build();
    // When: the backstop sweeps at thirty seconds
    let result = job.fail_run_for_lost_claim(
        reclaim(run_id, ts(30)),
        TimeDelta::seconds(60),
        &RetryPolicy::default(),
        &ServiceLimits::default(),
    );
    // Then: the claim still holds
    assert_eq!(
        result,
        Err(JobsError::ReservationWithinDelay {
            run_id: run_id.as_uuid()
        })
    );
}

#[test]
fn a_reservation_its_instance_started_late_is_not_lost() {
    // Given: a reserved run its instance started after the delay, before the sweep saw it
    let run = RunBuilder::new(1).reserved(ts(5)).started(ts(80)).build();
    let run_id = run.id();
    let job = JobBuilder::new().with_run(run).build();
    // When: the backstop reaches it
    let result = job
        .fail_run_for_lost_claim(
            reclaim(run_id, ts(90)),
            TimeDelta::seconds(60),
            &RetryPolicy::default(),
            &ServiceLimits::default(),
        )
        .unwrap();
    // Then: nothing happens — the claim was honoured
    assert!(result.is_empty());
}

#[test]
fn a_pushed_run_never_loses_a_claim_it_never_had() {
    // Given: a run published on the trigger stream and never started
    let run = RunBuilder::new(1).build();
    let run_id = run.id();
    let job = JobBuilder::new().with_run(run).build();
    // When: the lost-claim backstop is asked about it
    let result = job.fail_run_for_lost_claim(
        reclaim(run_id, ts(500)),
        TimeDelta::seconds(60),
        &RetryPolicy::default(),
        &ServiceLimits::default(),
    );
    // Then: it is refused — a pushed run waits on its trigger, not on a claim
    assert_eq!(
        result,
        Err(JobsError::RunNotReserved {
            run_id: run_id.as_uuid()
        })
    );
}

#[test]
fn a_run_dispatched_with_a_longer_maximum_outlives_the_service_default() {
    // Given: a run of a type allowing 96 hours, started at zero
    let run = RunBuilder::new(1)
        .with_max_duration(96 * 3600)
        .started(ts(0))
        .build();
    let run_id = run.id();
    let job = JobBuilder::new().with_run(run).build();
    // When: the backstop sweeps fifty hours later, past nothing
    let result = job.fail_run_for_max_duration(
        reclaim(run_id, ts(0) + TimeDelta::hours(50)),
        &RetryPolicy::default(),
        &ServiceLimits::default(),
    );
    // Then: the run is within its own maximum and is left alone
    assert_eq!(
        result,
        Err(JobsError::RunWithinMaxDuration {
            run_id: run_id.as_uuid()
        })
    );
}

#[test]
fn a_trigger_left_on_the_stream_of_a_claim_type_fails_as_stranded_and_waits_for_a_claim() {
    // Given: a pushed run never started, its type since switched to CLAIM
    let run = RunBuilder::new(1).build();
    let run_id = run.id();
    let job = JobBuilder::new().with_run(run).build();
    // When: the backstop sweeps past the delay
    let result = job
        .fail_run_for_stranded_trigger(
            reclaim(run_id, ts(70)),
            DispatchMode::Claim,
            TimeDelta::seconds(60),
            &RetryPolicy::default(),
            &ServiceLimits::default(),
        )
        .unwrap();
    // Then: any taker is told to stop, the run fails transient, and a retry waits
    match result.events.as_slice() {
        [
            JobEvent::RunCancellationRequested(requested),
            JobEvent::RunFailed(failed),
            JobEvent::RetryScheduled(_),
        ] => {
            assert_eq!(requested.reason_code.as_str(), "trigger_stranded");
            assert_eq!(failed.report.reason_code().as_str(), "trigger_stranded");
            assert_eq!(failed.report.kind(), RunFailureKind::Transient);
        }
        other => panic!("expected a stop, a transient failure and a retry, got {other:?}"),
    }
}

#[test]
fn a_trigger_of_a_push_type_or_within_the_delay_is_not_stranded() {
    // Given: a pushed run never started
    let run = RunBuilder::new(1).build();
    let run_id = run.id();
    let job = JobBuilder::new().with_run(run).build();
    // When/Then: on a PUSH type it is waiting for its taker, within the delay it is fresh
    assert_eq!(
        job.fail_run_for_stranded_trigger(
            reclaim(run_id, ts(70)),
            DispatchMode::Push,
            TimeDelta::seconds(60),
            &RetryPolicy::default(),
            &ServiceLimits::default(),
        )
        .unwrap_err()
        .code(),
        "not_claim_mode"
    );
    assert_eq!(
        job.fail_run_for_stranded_trigger(
            reclaim(run_id, ts(30)),
            DispatchMode::Claim,
            TimeDelta::seconds(60),
            &RetryPolicy::default(),
            &ServiceLimits::default(),
        ),
        Err(JobsError::TriggerNotStranded {
            run_id: run_id.as_uuid()
        })
    );
}
