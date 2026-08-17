use super::*;
use crate::domain::fleet::RunnerTypeState;
use crate::domain::fleet::capacity::Capacity;
use crate::domain::fleet::instance::RunnerInstanceState;
use crate::fixtures::ts;
use uuid::Uuid;

fn session_id() -> PresenceSessionId {
    PresenceSessionId::new(Uuid::now_v7()).unwrap()
}

fn presence(status: ReportedStatus) -> ObservePresence {
    declaring(status, 1)
}

fn declaring(status: ReportedStatus, capacity: u32) -> ObservePresence {
    ObservePresence {
        runner_type_id: RunnerTypeId::new(Uuid::now_v7()).unwrap(),
        runner_type: RunnerTypeKey::new("analyst").unwrap(),
        instance_key: InstanceKey::new("pod-7").unwrap(),
        session_id: session_id(),
        version: RunnerVersion::new("1.4.2").unwrap(),
        reported_status: status,
        capacity: Capacity::new(capacity).unwrap(),
    }
}

fn fleet(instances: Vec<RunnerInstance>) -> RunnerType {
    RunnerType::hydrate(RunnerTypeState {
        id: RunnerTypeId::new(Uuid::now_v7()).unwrap(),
        key: RunnerTypeKey::new("analyst").unwrap(),
        registered_at: ts(0),
        instances,
    })
    .unwrap()
}

fn live(status: ReportedStatus, changes: u32) -> RunnerInstance {
    live_with(status, changes, 1)
}

fn live_with(status: ReportedStatus, changes: u32, capacity: u32) -> RunnerInstance {
    RunnerInstance::hydrate(RunnerInstanceState {
        key: InstanceKey::new("pod-7").unwrap(),
        session_id: session_id(),
        version: RunnerVersion::new("1.4.2").unwrap(),
        reported_status: status,
        capacity: Capacity::new(capacity).unwrap(),
        connected_at: ts(1),
        last_observed_at: ts(6),
        status_change_number: changes,
    })
    .unwrap()
}

#[test]
fn the_first_presence_of_an_unknown_type_registers_it() {
    // Given: a runner type this service has never seen
    // When: one of its instances announces itself
    let result = observe_presence(None, presence(ReportedStatus::Ready)).unwrap();
    // Then: the type appears, together with its first live instance
    match result.events.as_slice() {
        [
            FleetEvent::RunnerTypeRegistered(registered),
            FleetEvent::InstanceConnected(connected),
        ] => {
            assert_eq!(registered.runner_type.as_str(), "analyst");
            assert_eq!(connected.instance_key.as_str(), "pod-7");
        }
        other => panic!("expected a registration and a connection, got {other:?}"),
    }
}

#[test]
fn a_new_instance_of_a_known_type_only_connects() {
    // Given: a known runner type with no live instance left
    let known = fleet(vec![]);
    // When: an instance announces itself, carrying an id the caller minted before the lookup
    let result = observe_presence(Some(&known), presence(ReportedStatus::Ready)).unwrap();
    // Then: the connection is filed under the type already registered, never under that id
    assert_eq!(result.events.len(), 1);
    match result.events.first() {
        Some(FleetEvent::InstanceConnected(fact)) => {
            assert_eq!(fact.runner_type_id, known.id());
            assert_eq!(&fact.runner_type, known.key());
        }
        other => panic!("expected a connection, got {other:?}"),
    }
}

#[test]
fn a_heartbeat_repeating_the_same_report_records_nothing() {
    // Given: a live instance reporting READY
    let known = fleet(vec![live(ReportedStatus::Ready, 0)]);
    // When: its heartbeat repeats the same report
    let result = observe_presence(Some(&known), presence(ReportedStatus::Ready)).unwrap();
    // Then: no fact is written for an unchanged heartbeat
    assert!(result.is_empty());
}

#[test]
fn a_changed_report_records_the_next_status_change() {
    // Given: a live instance that already changed status twice
    let known = fleet(vec![live(ReportedStatus::Ready, 2)]);
    // When: it reports that it is now draining
    let result = observe_presence(Some(&known), presence(ReportedStatus::Draining)).unwrap();
    // Then: the change is numbered after the last one
    match result.events.first() {
        Some(FleetEvent::InstanceStatusReported(fact)) => {
            assert_eq!(fact.reported_status, ReportedStatus::Draining);
            assert_eq!(fact.change_number, 3);
        }
        other => panic!("expected a status change, got {other:?}"),
    }
}

#[test]
fn a_capacity_change_alone_is_recorded_as_a_presence_change() {
    // Given: a live instance that had declared room for two runs
    let known = fleet(vec![live_with(ReportedStatus::Ready, 3, 2)]);
    // When: its next heartbeat declares room for four, everything else unchanged
    let result = observe_presence(Some(&known), declaring(ReportedStatus::Ready, 4)).unwrap();
    // Then: presence is one self-declared record, so a wider room is the same kind of fact
    // as a status change and is numbered in the same series
    match result.events.as_slice() {
        [FleetEvent::InstanceStatusReported(fact)] => {
            assert_eq!(fact.capacity, Capacity::new(4).unwrap());
            assert_eq!(fact.reported_status, ReportedStatus::Ready);
            assert_eq!(fact.change_number, 4);
        }
        other => panic!("expected one presence change, got {other:?}"),
    }
}

#[test]
fn a_heartbeat_repeating_the_same_capacity_records_nothing() {
    // Given: a live instance declaring room for two runs
    let known = fleet(vec![live_with(ReportedStatus::Ready, 0, 2)]);
    // When: its heartbeat repeats the identical declaration
    let result = observe_presence(Some(&known), declaring(ReportedStatus::Ready, 2)).unwrap();
    // Then: an unchanged refresh writes no fact, capacity included
    assert!(result.is_empty());
}

#[test]
fn a_drain_report_keeps_the_instance_live_and_never_closes_its_session() {
    // Given: a live instance holding its session
    let instance = live(ReportedStatus::Ready, 0);
    let session = instance.session_id();
    let known = fleet(vec![instance]);
    // When: it announces that it is winding down
    let result = observe_presence(Some(&known), presence(ReportedStatus::Draining)).unwrap();
    // Then: draining is a status change on the same session, never a disconnection —
    // the runs it already carries are not orphaned and their facts keep their instance
    match result.events.as_slice() {
        [FleetEvent::InstanceStatusReported(fact)] => {
            assert_eq!(fact.session_id, session);
            assert_eq!(fact.reported_status, ReportedStatus::Draining);
        }
        other => panic!("expected one status change and no disconnection, got {other:?}"),
    }
}

#[test]
fn a_draining_instance_that_reports_ready_again_is_the_same_session() {
    // Given: an instance that drained and is taking work again
    let instance = live(ReportedStatus::Draining, 4);
    let session = instance.session_id();
    let known = fleet(vec![instance]);
    // When: it reports READY
    let result = observe_presence(Some(&known), presence(ReportedStatus::Ready)).unwrap();
    // Then: it never reconnects — it just changes status, so availability returns at once
    match result.events.as_slice() {
        [FleetEvent::InstanceStatusReported(fact)] => {
            assert_eq!(fact.session_id, session);
            assert_eq!(fact.reported_status, ReportedStatus::Ready);
            assert_eq!(fact.change_number, 5);
        }
        other => panic!("expected one status change, got {other:?}"),
    }
}

#[test]
fn losing_an_instance_closes_the_session_it_had_open() {
    // Given: a live instance
    let instance = live(ReportedStatus::Draining, 1);
    let session = instance.session_id();
    let known = fleet(vec![instance]);
    // When: its presence entry expires
    let result = observe_loss(
        &known,
        ObserveLoss {
            instance_key: InstanceKey::new("pod-7").unwrap(),
            reason_code: ReasonCode::new(PRESENCE_EXPIRED).unwrap(),
        },
    )
    .unwrap();
    // Then: the disconnection names the session and why it ended
    match result.events.first() {
        Some(FleetEvent::InstanceDisconnected(fact)) => {
            assert_eq!(fact.session_id, session);
            assert_eq!(fact.reason_code.as_str(), "presence_expired");
        }
        other => panic!("expected a disconnection, got {other:?}"),
    }
}

#[test]
fn losing_an_instance_that_is_not_live_is_refused() {
    // Given: a runner type with no live instance
    let known = fleet(vec![]);
    // When: a loss is observed for an instance nobody tracks
    let result = observe_loss(
        &known,
        ObserveLoss {
            instance_key: InstanceKey::new("pod-9").unwrap(),
            reason_code: ReasonCode::new(GRACEFUL_SHUTDOWN).unwrap(),
        },
    );
    // Then: it is refused rather than inventing a session to close
    assert_eq!(
        result,
        Err(JobsError::InstanceNotLive {
            instance_key: "pod-9".to_owned()
        })
    );
}
