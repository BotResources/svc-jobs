use super::*;
use crate::domain::fleet::RunnerTypeState;
use crate::domain::fleet::capacity::Capacity;
use crate::domain::fleet::dispatch_mode::{DispatchMode, SuccessorDispatch};
use crate::domain::fleet::instance::{RunnerInstance, RunnerInstanceState};
use crate::domain::fleet::lifecycle::RunnerTypeLifecycle;
use crate::domain::fleet::run_duration::MaxRunDuration;
use crate::domain::fleet::settings::RunnerTypeSettings;
use crate::domain::fleet::status::ReportedStatus;
use crate::domain::ids::{PresenceSessionId, RunnerTypeId};
use crate::domain::keys::{InstanceKey, RunnerTypeKey, RunnerVersion};
use crate::fixtures::ts;
use uuid::Uuid;

fn runner_type(instances: Vec<RunnerInstance>) -> RunnerType {
    RunnerType::hydrate(RunnerTypeState {
        id: RunnerTypeId::new(Uuid::now_v7()).unwrap(),
        key: RunnerTypeKey::new("analyst").unwrap(),
        registered_at: ts(0),
        lifecycle: RunnerTypeLifecycle::Active,
        settings: RunnerTypeSettings::default(),
        instances,
    })
    .unwrap()
}

fn reporting(key: &str, status: ReportedStatus) -> RunnerInstance {
    RunnerInstance::hydrate(RunnerInstanceState {
        key: InstanceKey::new(key).unwrap(),
        session_id: PresenceSessionId::new(Uuid::now_v7()).unwrap(),
        version: RunnerVersion::new("1.4.2").unwrap(),
        reported_status: status,
        capacity: Capacity::new(1).unwrap(),
        connected_at: ts(1),
        last_observed_at: ts(6),
        status_change_number: 0,
    })
    .unwrap()
}

fn live() -> RunnerInstance {
    reporting("pod-7", ReportedStatus::Ready)
}

fn window(evaluated_at: chrono::DateTime<Utc>) -> RetirementWindow {
    RetirementWindow {
        evaluated_at,
        quiet_period: TimeDelta::hours(24),
    }
}

#[test]
fn dispatch_is_blocked_while_no_instance_is_live() {
    // Given: a runner type whose instances have all disconnected
    let empty = runner_type(vec![]);
    // When: the backend answers whether work may go out
    // Then: the wait is explained by a code, not inferred by the client
    assert_eq!(
        empty
            .affordances(RunnerTypeDecisionFacts::unused(window(ts(10))))
            .first()
            .unwrap()
            .reason_code(),
        Some("runner_type_unavailable")
    );
}

#[test]
fn dispatch_opens_as_soon_as_one_instance_is_live() {
    // Given: a runner type with one live instance
    let populated = runner_type(vec![live()]);
    // When/Then: dispatch is available
    assert_eq!(populated.can_dispatch(), Availability::Available);
}

#[test]
fn dispatch_waits_while_every_live_instance_is_draining() {
    // Given: a type whose only instances announced they are winding down
    let draining = runner_type(vec![
        reporting("pod-7", ReportedStatus::Draining),
        reporting("pod-8", ReportedStatus::Draining),
    ]);
    // When: the backend answers whether work may go out
    // Then: it waits, with the same code as an empty fleet — nobody would take the run,
    // so no trigger is published that would sit unclaimed
    assert_eq!(
        draining
            .affordances(RunnerTypeDecisionFacts::unused(window(ts(10))))
            .first()
            .unwrap()
            .reason_code(),
        Some("runner_type_unavailable")
    );
}

#[test]
fn dispatch_reopens_as_soon_as_a_draining_instance_reports_ready_again() {
    // Given: a drained fleet where one instance came back
    let recovered = runner_type(vec![
        reporting("pod-7", ReportedStatus::Draining),
        reporting("pod-8", ReportedStatus::Ready),
    ]);
    // When/Then: one taker is enough for work to flow again
    assert_eq!(recovered.can_dispatch(), Availability::Available);
}

#[test]
fn retirement_is_the_same_decision_used_by_the_affordance() {
    let mut state = RunnerTypeState {
        id: RunnerTypeId::new(Uuid::now_v7()).unwrap(),
        key: RunnerTypeKey::new("analyst").unwrap(),
        registered_at: ts(0),
        lifecycle: RunnerTypeLifecycle::Deprecated,
        settings: RunnerTypeSettings::default(),
        instances: vec![],
    };
    let facts = RunnerTypeDecisionFacts {
        non_terminal_job_count: 1,
        latest_terminal_run_at: None,
        window: window(ts(100)),
    };
    let runner_type = RunnerType::hydrate(state.clone()).unwrap();
    assert_eq!(
        runner_type.guard_retire(facts),
        Err(JobsError::RunnerTypeHasNonTerminalJobs { count: 1 })
    );
    assert_eq!(
        runner_type.can_retire(facts).reason_code(),
        Some("runner_type_has_non_terminal_jobs")
    );

    state.instances = vec![live()];
    state.lifecycle = RunnerTypeLifecycle::Retired;
    let retired = RunnerType::hydrate(state).unwrap();
    assert_eq!(retired.guard_reactivate(), Ok(()));
    assert!(retired.can_reactivate().is_available());
}

#[test]
fn reactivation_of_an_active_type_says_it_is_already_active() {
    // Given: an active runner type
    let active = runner_type(vec![live()]);
    // When: the backend answers whether it may be reactivated
    // Then: the code names the state it is in — a retired type is reactivatable too, so
    // "not deprecated" would have told the operator the opposite of the truth
    assert_eq!(
        active.can_reactivate().reason_code(),
        Some("runner_type_already_active")
    );
}

#[test]
fn the_retirement_quiet_period_is_the_configured_one() {
    // Given: a deprecated type whose last run terminated one hour before the decision
    let deprecated = RunnerType::hydrate(RunnerTypeState {
        id: RunnerTypeId::new(Uuid::now_v7()).unwrap(),
        key: RunnerTypeKey::new("analyst").unwrap(),
        registered_at: ts(0),
        lifecycle: RunnerTypeLifecycle::Deprecated,
        settings: RunnerTypeSettings::default(),
        instances: vec![],
    })
    .unwrap();
    let facts = |quiet_period| RunnerTypeDecisionFacts {
        non_terminal_job_count: 0,
        latest_terminal_run_at: Some(ts(0)),
        window: RetirementWindow {
            evaluated_at: ts(3_600),
            quiet_period,
        },
    };
    // When: the deployment configures a one-hour quiet period
    // Then: retirement is open, and a two-hour one moves the eligibility, not the rule
    assert_eq!(deprecated.guard_retire(facts(TimeDelta::hours(1))), Ok(()));
    assert_eq!(
        deprecated.guard_retire(facts(TimeDelta::hours(2))),
        Err(JobsError::RunnerTypeHasRecentTerminalRuns {
            eligible_at: ts(7_200)
        })
    );
}

fn claimed(instances: Vec<RunnerInstance>) -> RunnerType {
    RunnerType::hydrate(RunnerTypeState {
        id: RunnerTypeId::new(Uuid::now_v7()).unwrap(),
        key: RunnerTypeKey::new("analyst").unwrap(),
        registered_at: ts(0),
        lifecycle: RunnerTypeLifecycle::Active,
        settings: RunnerTypeSettings {
            dispatch_mode: DispatchMode::Claim,
            ..RunnerTypeSettings::default()
        },
        instances,
    })
    .unwrap()
}

fn pod(key: &str) -> InstanceKey {
    InstanceKey::new(key).unwrap()
}

#[test]
fn a_live_ready_instance_of_a_claim_type_may_claim() {
    // Given: a CLAIM type with pod-7 live and ready
    let runner_type = claimed(vec![live()]);
    // When/Then: pod-7's claim is served
    assert_eq!(
        runner_type
            .guard_claim(&pod("pod-7"))
            .map(|live| live.key().clone()),
        Ok(pod("pod-7"))
    );
}

#[test]
fn a_claim_on_a_type_pushed_to_is_refused_as_not_claim_mode() {
    // Given: a PUSH type with pod-7 live
    let runner_type = runner_type(vec![live()]);
    // When/Then: the claim is refused with the published code
    let refusal = runner_type.guard_claim(&pod("pod-7")).unwrap_err();
    assert_eq!(refusal.code(), "not_claim_mode");
}

#[test]
fn a_claim_from_an_instance_without_live_presence_is_refused_as_unknown() {
    // Given: a CLAIM type where only pod-7 is live
    let runner_type = claimed(vec![live()]);
    // When/Then: pod-9's claim is refused with the published code
    assert_eq!(
        runner_type.guard_claim(&pod("pod-9")).unwrap_err(),
        JobsError::InstanceUnknown {
            instance_key: "pod-9".to_owned()
        }
    );
}

#[test]
fn a_draining_instance_is_handed_no_new_run() {
    // Given: a CLAIM type whose only instance is draining
    let runner_type = claimed(vec![reporting("pod-7", ReportedStatus::Draining)]);
    // When/Then: its claim is refused — draining means finishing what it holds
    assert_eq!(
        runner_type.guard_claim(&pod("pod-7")).unwrap_err().code(),
        "instance_draining"
    );
}

#[test]
fn a_claim_type_is_never_pushed_to() {
    // Given: an available CLAIM type
    let runner_type = claimed(vec![live()]);
    // When/Then: dispatch is available, but publishing a trigger is not
    assert_eq!(runner_type.guard_dispatch(), Ok(()));
    assert_eq!(
        runner_type.guard_push_dispatch().unwrap_err().code(),
        "not_push_mode"
    );
    assert_eq!(
        self::runner_type(vec![live()]).guard_push_dispatch(),
        Ok(())
    );
}

#[test]
fn the_settings_affordances_are_open_whatever_the_lifecycle() {
    // Given: a type with no live instance
    let runner_type = runner_type(vec![]);
    // When: its affordances are projected
    let affordances = runner_type.affordances(RunnerTypeDecisionFacts::unused(window(ts(10))));
    // Then: both settings may be changed
    for action in [SET_DISPATCH_MODE, SET_MAX_RUN_DURATION] {
        let entry = affordances
            .iter()
            .find(|entry| entry.action() == action)
            .unwrap_or_else(|| panic!("{action} is projected"));
        assert!(entry.allowed(), "{action} is open");
    }
}

#[test]
fn the_successor_of_a_claim_type_waits_to_be_claimed_and_any_other_is_pushed() {
    // Given: a CLAIM type, a PUSH type set to 96 hours, and no registered type
    let run = crate::fixtures::run_id();
    let pushed = RunnerType::hydrate(RunnerTypeState {
        id: RunnerTypeId::new(Uuid::now_v7()).unwrap(),
        key: RunnerTypeKey::new("analyst").unwrap(),
        registered_at: ts(0),
        lifecycle: RunnerTypeLifecycle::Active,
        settings: RunnerTypeSettings {
            max_run_duration: Some(MaxRunDuration::from_seconds(96 * 3600).unwrap()),
            ..RunnerTypeSettings::default()
        },
        instances: vec![],
    })
    .unwrap();
    // When/Then: the decision follows the type's mode and duration
    assert_eq!(
        successor_dispatch(Some(&claimed(vec![])), run, MaxRunDuration::SERVICE_DEFAULT),
        SuccessorDispatch::AwaitClaim
    );
    assert_eq!(
        successor_dispatch(Some(&pushed), run, MaxRunDuration::SERVICE_DEFAULT),
        SuccessorDispatch::Push {
            first_run_id: run,
            max_run_duration: MaxRunDuration::from_seconds(96 * 3600).unwrap(),
        }
    );
    assert_eq!(
        successor_dispatch(None, run, MaxRunDuration::SERVICE_DEFAULT),
        SuccessorDispatch::Push {
            first_run_id: run,
            max_run_duration: MaxRunDuration::SERVICE_DEFAULT,
        }
    );
}
