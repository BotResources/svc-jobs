use super::*;
use crate::domain::fleet::run_duration::MaxRunDuration;
use crate::domain::keys::{InstanceKey, ReasonCode, RunnerTypeKey};
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
    let run = Run::hydrate(pending()).unwrap();
    assert_eq!(run.status(), RunStatus::Pending);

    let started = Run::hydrate(RunState {
        start: Some(RunStart::new(instance(), at(5))),
        ..pending()
    })
    .unwrap();
    assert_eq!(started.status(), RunStatus::Started);

    let finished = Run::hydrate(RunState {
        start: Some(RunStart::new(instance(), at(5))),
        terminal: Some(RunTerminal::completed(at(9))),
        ..pending()
    })
    .unwrap();
    assert_eq!(finished.status(), RunStatus::Completed);
}

#[test]
fn a_run_that_started_before_it_was_dispatched_cannot_be_loaded() {
    let result = Run::hydrate(RunState {
        start: Some(RunStart::new(instance(), at(-5))),
        ..pending()
    });
    assert_eq!(
        result.err(),
        Some(JobsError::CorruptState {
            reason_code: "run_started_before_dispatch"
        })
    );
}

#[test]
fn a_retry_schedule_hanging_off_a_completed_run_cannot_be_loaded() {
    let schedule = RetrySchedule::new(RetryScheduleId::new(Uuid::now_v7()).unwrap(), at(60));
    let result = Run::hydrate(RunState {
        start: Some(RunStart::new(instance(), at(5))),
        terminal: Some(RunTerminal::completed(at(9))),
        retry_schedule: Some(schedule),
        ..pending()
    });
    assert_eq!(
        result.err(),
        Some(JobsError::CorruptState {
            reason_code: "retry_scheduled_without_transient_failure"
        })
    );
}

#[test]
fn a_retry_schedule_hanging_off_a_permanent_failure_cannot_be_loaded() {
    let report =
        RunFailureReport::platform(RunFailureKind::Permanent, "configuration_invalid").unwrap();
    let schedule = RetrySchedule::new(RetryScheduleId::new(Uuid::now_v7()).unwrap(), at(60));
    let result = Run::hydrate(RunState {
        terminal: Some(RunTerminal::failed(at(9), report)),
        retry_schedule: Some(schedule),
        ..pending()
    });
    assert!(result.is_err());
}

#[test]
fn a_first_attempt_carrying_a_retry_schedule_reference_cannot_be_loaded() {
    let result = Run::hydrate(RunState {
        automatic_retry_schedule_id: Some(RetryScheduleId::new(Uuid::now_v7()).unwrap()),
        ..pending()
    });
    assert_eq!(
        result.err(),
        Some(JobsError::CorruptState {
            reason_code: "retry_schedule_does_not_match_attempt_number"
        })
    );
}

#[test]
fn a_started_run_past_the_maximum_duration_has_outrun_its_budget() {
    let run = Run::hydrate(RunState {
        start: Some(RunStart::new(instance(), at(0))),
        ..pending()
    })
    .unwrap();
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
    let run = Run::hydrate(RunState {
        max_duration: Some(MaxRunDuration::from_seconds(96 * 3600).unwrap()),
        start: Some(RunStart::new(instance(), at(0))),
        ..pending()
    })
    .unwrap();
    assert!(!run.has_outrun(at(50 * 3600), TimeDelta::hours(72)));
    assert!(run.has_outrun(at(97 * 3600), TimeDelta::hours(72)));
}

#[test]
fn a_run_dispatched_before_runs_carried_a_duration_answers_to_the_service_default() {
    let run = Run::hydrate(RunState {
        start: Some(RunStart::new(instance(), at(0))),
        ..pending()
    })
    .unwrap();
    assert!(run.has_outrun(at(73 * 3600), TimeDelta::hours(72)));
    assert!(!run.has_outrun(at(71 * 3600), TimeDelta::hours(72)));
}

#[test]
fn a_reserved_run_belongs_to_its_claimant_before_it_starts() {
    let run = Run::hydrate(reserved_at(1)).unwrap();
    assert_eq!(run.status(), RunStatus::Pending);
    assert!(run.is_reserved());
    assert!(run.is_held_by(&instance()));
    assert!(run.is_held_by_an_instance());
    assert_eq!(run.instance(), Some(&instance()));
}

#[test]
fn a_reserved_run_never_started_loses_its_claim_past_the_delay() {
    let run = Run::hydrate(reserved_at(1)).unwrap();
    assert!(!run.has_lost_its_claim(at(30), TimeDelta::seconds(60)));
    assert!(run.has_lost_its_claim(at(62), TimeDelta::seconds(60)));
}

#[test]
fn a_started_or_settled_reserved_run_never_loses_its_claim() {
    let started = Run::hydrate(RunState {
        start: Some(RunStart::new(instance(), at(5))),
        ..reserved_at(1)
    })
    .unwrap();
    assert!(!started.has_lost_its_claim(at(10_000), TimeDelta::seconds(60)));
}

#[test]
fn a_reserved_run_started_by_another_instance_cannot_be_loaded() {
    let elsewhere = RunnerInstanceReference::new(
        RunnerTypeKey::new("analyst").unwrap(),
        InstanceKey::new("pod-9").unwrap(),
    );
    let result = Run::hydrate(RunState {
        start: Some(RunStart::new(elsewhere, at(5))),
        ..reserved_at(1)
    });
    assert_eq!(
        result.err(),
        Some(JobsError::CorruptState {
            reason_code: "run_started_by_another_instance_than_its_claimant"
        })
    );
}

#[test]
fn a_reserved_run_may_fail_without_ever_starting_while_a_pushed_one_may_not() {
    let report = RunFailureReport::platform(RunFailureKind::Transient, "claim_lost").unwrap();
    let reserved = Run::hydrate(RunState {
        terminal: Some(RunTerminal::failed(at(90), report.clone())),
        ..reserved_at(1)
    });
    let pushed = Run::hydrate(RunState {
        terminal: Some(RunTerminal::failed(at(90), report)),
        ..pending()
    });
    assert!(reserved.is_ok());
    assert_eq!(
        pushed.err(),
        Some(JobsError::CorruptState {
            reason_code: "run_settled_without_start"
        })
    );
}

#[test]
fn a_pushed_run_with_no_passport_to_send_fails_unstarted_and_loads() {
    let report =
        RunFailureReport::platform(RunFailureKind::Permanent, "passport_required").unwrap();
    let pushed = Run::hydrate(RunState {
        terminal: Some(RunTerminal::failed(at(90), report)),
        ..pending()
    });
    assert!(pushed.is_ok());
}

#[test]
fn a_pushed_run_withdrawn_unstarted_loads_and_does_not_count_while_a_taken_one_cannot_be_withdrawn()
{
    let withdrawn = Run::hydrate(RunState {
        terminal: Some(RunTerminal::withdrawn(
            at(90),
            ReasonCode::new("trigger_stranded").unwrap(),
        )),
        ..pending()
    })
    .unwrap();
    assert!(!withdrawn.counts_as_attempt());
    let taken = Run::hydrate(RunState {
        terminal: Some(RunTerminal::withdrawn(
            at(90),
            ReasonCode::new("trigger_stranded").unwrap(),
        )),
        ..reserved_at(1)
    });
    assert_eq!(
        taken.err(),
        Some(JobsError::CorruptState {
            reason_code: "withdrawn_run_was_taken"
        })
    );
}
