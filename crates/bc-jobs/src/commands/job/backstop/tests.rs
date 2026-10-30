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
    let run = RunBuilder::new(1).started(ts(5)).build();
    let run_id = run.id();
    let job = JobBuilder::new().with_run(run).build();
    let result = job
        .fail_run_for_instance_loss(
            reclaim(run_id, ts(50)),
            &RetryPolicy::default(),
            &ServiceLimits::default(),
        )
        .unwrap();
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
    let run = RunBuilder::new(1).started(ts(0)).build();
    let run_id = run.id();
    let job = JobBuilder::new().with_run(run).build();
    let past_the_ceiling = ts(0) + TimeDelta::hours(80);
    let result = job
        .fail_run_for_max_duration(
            reclaim(run_id, past_the_ceiling),
            &RetryPolicy::default(),
            &ServiceLimits::default(),
        )
        .unwrap();
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
    let run = RunBuilder::new(1).started(ts(0)).build();
    let run_id = run.id();
    let job = JobBuilder::new().with_run(run).build();
    let result = job.fail_run_for_max_duration(
        reclaim(run_id, ts(60)),
        &RetryPolicy::default(),
        &ServiceLimits::default(),
    );
    assert_eq!(
        result,
        Err(JobsError::RunWithinMaxDuration {
            run_id: run_id.as_uuid()
        })
    );
}

#[test]
fn a_job_abandoned_by_its_owner_fails_with_the_inactivity_cause() {
    let job = JobBuilder::new()
        .with_run(RunBuilder::new(1).started(ts(5)).completed(ts(20)).build())
        .build();
    let long_after = ts(20) + TimeDelta::hours(30);
    let result = job
        .fail_for_inactivity(
            FailForInactivity {
                resolution_id: resolution_id(),
                at: long_after,
            },
            &ServiceLimits::default(),
        )
        .unwrap();
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
    let job = JobBuilder::new()
        .with_run(RunBuilder::new(1).started(ts(5)).build())
        .build();
    let result = job.fail_for_inactivity(
        FailForInactivity {
            resolution_id: resolution_id(),
            at: ts(5) + TimeDelta::hours(30),
        },
        &ServiceLimits::default(),
    );
    assert_eq!(result, Err(JobsError::JobStillActive));
}

#[test]
fn a_job_with_a_scheduled_retry_is_never_reclaimed_for_inactivity() {
    let job = JobBuilder::new()
        .with_run(
            RunBuilder::new(1)
                .started(ts(5))
                .failed(ts(20), RunFailureKind::Transient)
                .retry_due(ts(80))
                .build(),
        )
        .build();
    let result = job.fail_for_inactivity(
        FailForInactivity {
            resolution_id: resolution_id(),
            at: ts(80) + TimeDelta::hours(30),
        },
        &ServiceLimits::default(),
    );
    assert_eq!(result, Err(JobsError::JobStillActive));
}

#[test]
fn a_job_idle_for_less_than_the_window_is_left_alone() {
    let job = JobBuilder::new()
        .with_run(RunBuilder::new(1).started(ts(5)).completed(ts(20)).build())
        .build();
    let result = job.fail_for_inactivity(
        FailForInactivity {
            resolution_id: resolution_id(),
            at: ts(20) + TimeDelta::hours(1),
        },
        &ServiceLimits::default(),
    );
    assert_eq!(
        result,
        Err(JobsError::InactivityTimeoutNotReached { idle_since: ts(20) })
    );
}

#[test]
fn a_pending_job_is_not_reclaimed_for_inactivity() {
    let job = JobBuilder::new().build();
    let result = job.fail_for_inactivity(
        FailForInactivity {
            resolution_id: resolution_id(),
            at: ts(0) + TimeDelta::hours(300),
        },
        &ServiceLimits::default(),
    );
    assert_eq!(
        result,
        Err(JobsError::JobNotInProgress { status: "PENDING" })
    );
}

#[test]
fn a_reservation_never_started_fails_transiently_with_claim_lost_and_the_job_waits_again() {
    let run = RunBuilder::new(1).reserved(ts(5)).build();
    let run_id = run.id();
    let job = JobBuilder::new().with_run(run).build();
    let result = job
        .fail_run_for_lost_claim(
            reclaim(run_id, ts(70)),
            TimeDelta::seconds(60),
            &RetryPolicy::default(),
            &ServiceLimits::default(),
        )
        .unwrap();
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
    let run = RunBuilder::new(1).reserved(ts(5)).build();
    let run_id = run.id();
    let job = JobBuilder::new().with_run(run).build();
    let result = job.fail_run_for_lost_claim(
        reclaim(run_id, ts(30)),
        TimeDelta::seconds(60),
        &RetryPolicy::default(),
        &ServiceLimits::default(),
    );
    assert_eq!(
        result,
        Err(JobsError::ReservationWithinDelay {
            run_id: run_id.as_uuid()
        })
    );
}

#[test]
fn a_reservation_its_instance_started_late_is_not_lost() {
    let run = RunBuilder::new(1).reserved(ts(5)).started(ts(80)).build();
    let run_id = run.id();
    let job = JobBuilder::new().with_run(run).build();
    let result = job
        .fail_run_for_lost_claim(
            reclaim(run_id, ts(90)),
            TimeDelta::seconds(60),
            &RetryPolicy::default(),
            &ServiceLimits::default(),
        )
        .unwrap();
    assert!(result.is_empty());
}

#[test]
fn a_pushed_run_never_loses_a_claim_it_never_had() {
    let run = RunBuilder::new(1).build();
    let run_id = run.id();
    let job = JobBuilder::new().with_run(run).build();
    let result = job.fail_run_for_lost_claim(
        reclaim(run_id, ts(500)),
        TimeDelta::seconds(60),
        &RetryPolicy::default(),
        &ServiceLimits::default(),
    );
    assert_eq!(
        result,
        Err(JobsError::RunNotReserved {
            run_id: run_id.as_uuid()
        })
    );
}

#[test]
fn a_run_dispatched_with_a_longer_maximum_outlives_the_service_default() {
    let run = RunBuilder::new(1)
        .with_max_duration(96 * 3600)
        .started(ts(0))
        .build();
    let run_id = run.id();
    let job = JobBuilder::new().with_run(run).build();
    let result = job.fail_run_for_max_duration(
        reclaim(run_id, ts(0) + TimeDelta::hours(50)),
        &RetryPolicy::default(),
        &ServiceLimits::default(),
    );
    assert_eq!(
        result,
        Err(JobsError::RunWithinMaxDuration {
            run_id: run_id.as_uuid()
        })
    );
}

#[test]
fn a_trigger_stranded_by_a_switch_to_claim_is_withdrawn_without_spending_an_attempt() {
    let run = RunBuilder::new(1).build();
    let run_id = run.id();
    let job = JobBuilder::new().with_run(run).build();
    let result = job
        .withdraw_stranded_trigger(
            WithdrawStrandedTrigger { run_id, at: ts(80) },
            Some(ts(10)),
            TimeDelta::seconds(60),
        )
        .unwrap();
    match result.events.as_slice() {
        [
            JobEvent::RunCancellationRequested(requested),
            JobEvent::RunCancelled(cancelled),
        ] => {
            assert_eq!(requested.reason_code.as_str(), "trigger_stranded");
            assert_eq!(
                cancelled.withdrawn_for.as_ref().map(ReasonCode::as_str),
                Some("trigger_stranded")
            );
        }
        other => panic!("expected a stop and a withdrawal, got {other:?}"),
    }
}

#[test]
fn a_withdrawn_run_leaves_its_job_waiting_with_no_attempt_spent() {
    let run = RunBuilder::new(1)
        .withdrawn(ts(80), "trigger_stranded")
        .build();
    let job = JobBuilder::new().with_run(run).build();
    assert!(job.is_waiting());
    assert_eq!(job.attempt_count(), 0);
    assert_eq!(job.attempts_reached(), 1);
}

#[test]
fn a_trigger_is_stranded_only_on_a_claim_type_and_only_past_the_delay_after_the_switch() {
    let run = RunBuilder::new(1).build();
    let run_id = run.id();
    let job = JobBuilder::new().with_run(run).build();
    let at = |offset| WithdrawStrandedTrigger {
        run_id,
        at: ts(offset),
    };
    assert_eq!(
        job.withdraw_stranded_trigger(at(500), None, TimeDelta::seconds(60))
            .unwrap_err()
            .code(),
        "not_claim_mode"
    );
    assert_eq!(
        job.withdraw_stranded_trigger(at(130), Some(ts(100)), TimeDelta::seconds(60)),
        Err(JobsError::TriggerNotStranded {
            run_id: run_id.as_uuid()
        })
    );
    assert!(
        job.withdraw_stranded_trigger(at(161), Some(ts(100)), TimeDelta::seconds(60))
            .is_ok()
    );
}

#[test]
fn a_run_whose_job_holds_no_passport_fails_permanently_so_no_retry_follows() {
    let run = RunBuilder::new(1).build();
    let run_id = run.id();
    let job = JobBuilder::new().without_passport().with_run(run).build();
    let result = job
        .fail_run_for_missing_passport(
            reclaim(run_id, ts(50)),
            &RetryPolicy::default(),
            &ServiceLimits::default(),
        )
        .unwrap();
    match result.events.as_slice() {
        [JobEvent::RunFailed(failed), JobEvent::JobFailed(_)] => {
            assert_eq!(failed.report.reason_code().as_str(), "passport_required");
            assert_eq!(failed.report.kind(), RunFailureKind::Permanent);
        }
        other => panic!("expected a permanent failure that ends the job, got {other:?}"),
    }
}
