use super::*;
use crate::domain::fleet::RunnerTypeState;
use crate::domain::fleet::capacity::Capacity;
use crate::domain::fleet::instance::{RunnerInstance, RunnerInstanceState};
use crate::domain::fleet::lifecycle::RunnerTypeLifecycle;
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
