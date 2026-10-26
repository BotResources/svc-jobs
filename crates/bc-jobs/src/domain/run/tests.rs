use super::*;
use crate::domain::fleet::run_duration::MaxRunDuration;
use crate::domain::keys::{InstanceKey, RunnerTypeKey};
use crate::domain::run::failure::{RunFailureKind, RunFailureReport};
use crate::domain::run::parts::{RunReservation, RunnerInstanceReference};
use uuid::Uuid;

fn at(offset: i64) -> DateTime<Utc> {
    DateTime::from_timestamp(1_700_000_000 + offset, 0).unwrap()
}

fn run_id() -> RunId {
    RunId::new(Uuid::now_v7()).unwrap()
}

fn instance() -> RunnerInstanceReference {
    RunnerInstanceReference::new(
        RunnerTypeKey::new("analyst").unwrap(),
        InstanceKey::new("pod-7").unwrap(),
    )
}

fn pending() -> RunState {
    RunState {
        id: run_id(),
        attempt_number: AttemptNumber::FIRST,
        dispatched_at: at(0),
        automatic_retry_schedule_id: None,
        max_duration: None,
        reservation: None,
        start: None,
        terminal: None,
        plan: None,
        steps: vec![],
        retry_schedule: None,
        cancellation_request: None,
    }
}

#[test]
fn a_run_status_is_derived_from_its_lifecycle_facts() {
    // Given: a dispatched run with no further fact
    let run = Run::hydrate(pending()).unwrap();
    // Then: it is pending
    assert_eq!(run.status(), RunStatus::Pending);

    // When: an instance claims it
    let started = Run::hydrate(RunState {
        start: Some(RunStart::new(instance(), at(5))),
        ..pending()
    })
    .unwrap();
    // Then: it is started
    assert_eq!(started.status(), RunStatus::Started);

    // When: it finishes
    let finished = Run::hydrate(RunState {
        start: Some(RunStart::new(instance(), at(5))),
        terminal: Some(RunTerminal::completed(at(9))),
        ..pending()
    })
    .unwrap();
    // Then: the terminal fact governs
    assert_eq!(finished.status(), RunStatus::Completed);
}

#[test]
fn a_run_that_started_before_it_was_dispatched_cannot_be_loaded() {
    // Given: a stored run whose start predates its dispatch
    let result = Run::hydrate(RunState {
        start: Some(RunStart::new(instance(), at(-5))),
        ..pending()
    });
    // Then: the impossible history is refused at load
    assert_eq!(
        result.err(),
        Some(JobsError::CorruptState {
            reason_code: "run_started_before_dispatch"
        })
    );
}

#[test]
fn a_retry_schedule_hanging_off_a_completed_run_cannot_be_loaded() {
    // Given: a stored run that completed yet carries a retry schedule
    let schedule = RetrySchedule::new(RetryScheduleId::new(Uuid::now_v7()).unwrap(), at(60));
    let result = Run::hydrate(RunState {
        start: Some(RunStart::new(instance(), at(5))),
        terminal: Some(RunTerminal::completed(at(9))),
        retry_schedule: Some(schedule),
        ..pending()
    });
    // Then: retry-after-success is refused at load
    assert_eq!(
        result.err(),
        Some(JobsError::CorruptState {
            reason_code: "retry_scheduled_without_transient_failure"
        })
    );
}

#[test]
fn a_retry_schedule_hanging_off_a_permanent_failure_cannot_be_loaded() {
    // Given: a stored run that failed permanently yet carries a retry schedule
    let report =
        RunFailureReport::platform(RunFailureKind::Permanent, "configuration_invalid").unwrap();
    let schedule = RetrySchedule::new(RetryScheduleId::new(Uuid::now_v7()).unwrap(), at(60));
    let result = Run::hydrate(RunState {
        terminal: Some(RunTerminal::failed(at(9), report)),
        retry_schedule: Some(schedule),
        ..pending()
    });
    // Then: a permanent failure ends retry, at write time and at load time
    assert!(result.is_err());
}

#[test]
fn a_first_attempt_carrying_a_retry_schedule_reference_cannot_be_loaded() {
    // Given: a stored attempt one claiming to descend from a retry schedule
    let result = Run::hydrate(RunState {
        automatic_retry_schedule_id: Some(RetryScheduleId::new(Uuid::now_v7()).unwrap()),
        ..pending()
    });
    // Then: the contradiction is refused
    assert_eq!(
        result.err(),
        Some(JobsError::CorruptState {
            reason_code: "retry_schedule_does_not_match_attempt_number"
        })
    );
}

#[test]
fn a_started_run_past_the_maximum_duration_has_outrun_its_budget() {
    // Given: a run started an hour ago with a thirty minute ceiling
    let run = Run::hydrate(RunState {
        start: Some(RunStart::new(instance(), at(0))),
        ..pending()
    })
    .unwrap();
    // When/Then: the ceiling is exceeded, while a pending run never is
    assert!(run.has_outrun(at(3600), TimeDelta::minutes(30)));
    assert!(!run.has_outrun(at(60), TimeDelta::minutes(30)));
    assert!(
        !Run::hydrate(pending())
            .unwrap()
            .has_outrun(at(3600), TimeDelta::minutes(30))
    );
}

fn reserved_at(offset: i64) -> RunState {
    RunState {
        reservation: Some(RunReservation::new(
            instance(),
            crate::domain::ids::ClaimId::new(Uuid::now_v7()).unwrap(),
            at(offset),
        )),
        ..pending()
    }
}

#[test]
fn a_run_carries_the_maximum_duration_it_was_dispatched_with_not_the_default() {
    // Given: a run dispatched for a type allowing 96 hours, started at zero
    let run = Run::hydrate(RunState {
        max_duration: Some(MaxRunDuration::from_seconds(96 * 3600).unwrap()),
        start: Some(RunStart::new(instance(), at(0))),
        ..pending()
    })
    .unwrap();
    // When: fifty hours have passed, while the service default is 72 hours
    // Then: the run is within its own budget
    assert!(!run.has_outrun(at(50 * 3600), TimeDelta::hours(72)));
    assert!(run.has_outrun(at(97 * 3600), TimeDelta::hours(72)));
}

#[test]
fn a_run_dispatched_before_runs_carried_a_duration_answers_to_the_service_default() {
    // Given: a legacy run, started at zero, with no duration of its own
    let run = Run::hydrate(RunState {
        start: Some(RunStart::new(instance(), at(0))),
        ..pending()
    })
    .unwrap();
    // When/Then: the service default governs it
    assert!(run.has_outrun(at(73 * 3600), TimeDelta::hours(72)));
    assert!(!run.has_outrun(at(71 * 3600), TimeDelta::hours(72)));
}

#[test]
fn a_reserved_run_belongs_to_its_claimant_before_it_starts() {
    // Given: a run reserved to pod-7 and not started
    let run = Run::hydrate(reserved_at(1)).unwrap();
    // When/Then: it is still pending, but pod-7 holds it
    assert_eq!(run.status(), RunStatus::Pending);
    assert!(run.is_reserved());
    assert!(run.is_held_by(&instance()));
    assert!(run.is_held_by_an_instance());
    assert_eq!(run.instance(), Some(&instance()));
}

#[test]
fn a_reserved_run_never_started_loses_its_claim_past_the_delay() {
    // Given: a run reserved at one second
    let run = Run::hydrate(reserved_at(1)).unwrap();
    // When/Then: within the delay it holds, past it the claim is lost
    assert!(!run.has_lost_its_claim(at(30), TimeDelta::seconds(60)));
    assert!(run.has_lost_its_claim(at(62), TimeDelta::seconds(60)));
}

#[test]
fn a_started_or_settled_reserved_run_never_loses_its_claim() {
    // Given: a reserved run its instance started
    let started = Run::hydrate(RunState {
        start: Some(RunStart::new(instance(), at(5))),
        ..reserved_at(1)
    })
    .unwrap();
    // When/Then: however late, the claim was honoured
    assert!(!started.has_lost_its_claim(at(10_000), TimeDelta::seconds(60)));
}

#[test]
fn a_reserved_run_started_by_another_instance_cannot_be_loaded() {
    // Given: a run reserved to pod-7 but started by pod-9
    let elsewhere = RunnerInstanceReference::new(
        RunnerTypeKey::new("analyst").unwrap(),
        InstanceKey::new("pod-9").unwrap(),
    );
    let result = Run::hydrate(RunState {
        start: Some(RunStart::new(elsewhere, at(5))),
        ..reserved_at(1)
    });
    // Then: the contradiction is refused at load
    assert_eq!(
        result.err(),
        Some(JobsError::CorruptState {
            reason_code: "run_started_by_another_instance_than_its_claimant"
        })
    );
}

#[test]
fn a_reserved_run_may_fail_without_ever_starting_while_a_pushed_one_may_not() {
    // Given: a transient failure report, as the lost-claim backstop records one
    let report = RunFailureReport::platform(RunFailureKind::Transient, "claim_lost").unwrap();
    // When: a reserved run and a pushed run each fail unstarted
    let reserved = Run::hydrate(RunState {
        terminal: Some(RunTerminal::failed(at(90), report.clone())),
        ..reserved_at(1)
    });
    let pushed = Run::hydrate(RunState {
        terminal: Some(RunTerminal::failed(at(90), report)),
        ..pending()
    });
    // Then: the reserved one loads — an instance held it — the pushed one does not
    assert!(reserved.is_ok());
    assert_eq!(
        pushed.err(),
        Some(JobsError::CorruptState {
            reason_code: "run_settled_without_start"
        })
    );
}

#[test]
fn a_pushed_run_may_fail_unstarted_only_as_a_stranded_trigger() {
    // Given: the failure the stranded-trigger backstop records
    let report = RunFailureReport::platform(RunFailureKind::Transient, "trigger_stranded").unwrap();
    // When: a pushed run that never started carries it
    let stranded = Run::hydrate(RunState {
        terminal: Some(RunTerminal::failed(at(90), report)),
        ..pending()
    });
    // Then: it loads — no instance ever took the trigger
    assert!(stranded.is_ok());
}
