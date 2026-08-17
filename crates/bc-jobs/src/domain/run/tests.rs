use super::*;
use crate::domain::keys::{InstanceKey, RunnerTypeKey};
use crate::domain::run::failure::{RunFailureKind, RunFailureReport};
use crate::domain::run::parts::RunnerInstanceReference;
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
