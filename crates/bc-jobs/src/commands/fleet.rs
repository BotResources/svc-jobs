use crate::commands::{CommandResult, CommandWarning, FleetCommandResult};
use crate::domain::fleet::RunnerType;
use crate::domain::fleet::instance::RunnerInstance;
use crate::domain::ids::{PresenceSessionId, RunnerTypeId};
use crate::domain::keys::{InstanceKey, ReasonCode, ReportedStatus, RunnerTypeKey, RunnerVersion};
use crate::error::JobsError;
use crate::event::fleet::{
    FleetEvent, InstanceConnected, InstanceDisconnected, InstanceStatusReported,
    RunnerTypeRegistered,
};

pub const PRESENCE_EXPIRED: &str = "presence_expired";
pub const GRACEFUL_SHUTDOWN: &str = "graceful_shutdown";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservePresence {
    pub runner_type_id: RunnerTypeId,
    pub runner_type: RunnerTypeKey,
    pub instance_key: InstanceKey,
    pub session_id: PresenceSessionId,
    pub version: RunnerVersion,
    pub reported_status: ReportedStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObserveLoss {
    pub instance_key: InstanceKey,
    pub reason_code: ReasonCode,
}

pub fn observe_presence(
    known: Option<&RunnerType>,
    command: ObservePresence,
) -> Result<FleetCommandResult, JobsError> {
    let Some(runner_type) = known else {
        return Ok(CommandResult::new(vec![
            FleetEvent::RunnerTypeRegistered(RunnerTypeRegistered {
                runner_type_id: command.runner_type_id,
                runner_type: command.runner_type.clone(),
            }),
            connected(&command),
        ]));
    };
    let Some(live) = runner_type.instance(&command.instance_key) else {
        return Ok(CommandResult::from_event(connected(&command)));
    };
    if live.reports_the_same_as(&command.version, &command.reported_status) {
        return Ok(CommandResult::nothing_happened(
            CommandWarning::FactAlreadyRecorded {
                fact: "InstanceStatusReported",
            },
        ));
    }
    Ok(CommandResult::from_event(
        FleetEvent::InstanceStatusReported(InstanceStatusReported {
            runner_type_id: runner_type.id(),
            runner_type: runner_type.key().clone(),
            instance_key: command.instance_key,
            session_id: live.session_id(),
            version: command.version,
            reported_status: command.reported_status,
            change_number: live.status_change_number().saturating_add(1),
        }),
    ))
}

pub fn observe_loss(
    runner_type: &RunnerType,
    command: ObserveLoss,
) -> Result<FleetCommandResult, JobsError> {
    let live: &RunnerInstance =
        runner_type
            .instance(&command.instance_key)
            .ok_or_else(|| JobsError::InstanceNotLive {
                instance_key: command.instance_key.as_str().to_owned(),
            })?;
    Ok(CommandResult::from_event(FleetEvent::InstanceDisconnected(
        InstanceDisconnected {
            runner_type_id: runner_type.id(),
            runner_type: runner_type.key().clone(),
            instance_key: command.instance_key.clone(),
            session_id: live.session_id(),
            reason_code: command.reason_code,
        },
    )))
}

fn connected(command: &ObservePresence) -> FleetEvent {
    FleetEvent::InstanceConnected(InstanceConnected {
        runner_type_id: command.runner_type_id,
        runner_type: command.runner_type.clone(),
        instance_key: command.instance_key.clone(),
        session_id: command.session_id,
        version: command.version.clone(),
        reported_status: command.reported_status.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::fleet::RunnerTypeState;
    use crate::fixtures::ts;
    use uuid::Uuid;

    fn session_id() -> PresenceSessionId {
        PresenceSessionId::new(Uuid::now_v7()).unwrap()
    }

    fn presence(status: &str) -> ObservePresence {
        ObservePresence {
            runner_type_id: RunnerTypeId::new(Uuid::now_v7()).unwrap(),
            runner_type: RunnerTypeKey::new("analyst").unwrap(),
            instance_key: InstanceKey::new("pod-7").unwrap(),
            session_id: session_id(),
            version: RunnerVersion::new("1.4.2").unwrap(),
            reported_status: ReportedStatus::new(status).unwrap(),
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

    fn live(status: &str, changes: u32) -> RunnerInstance {
        RunnerInstance::hydrate(
            InstanceKey::new("pod-7").unwrap(),
            session_id(),
            RunnerVersion::new("1.4.2").unwrap(),
            ReportedStatus::new(status).unwrap(),
            ts(1),
            ts(6),
            changes,
        )
        .unwrap()
    }

    #[test]
    fn the_first_presence_of_an_unknown_type_registers_it() {
        // Given: a runner type this service has never seen
        // When: one of its instances announces itself
        let result = observe_presence(None, presence("idle")).unwrap();
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
        // When: an instance announces itself
        let result = observe_presence(Some(&known), presence("idle")).unwrap();
        // Then: only the connection is recorded — the type is already registered
        assert_eq!(result.events.len(), 1);
        assert!(matches!(
            result.events.first(),
            Some(FleetEvent::InstanceConnected(_))
        ));
    }

    #[test]
    fn a_heartbeat_repeating_the_same_report_records_nothing() {
        // Given: a live instance reporting idle
        let known = fleet(vec![live("idle", 0)]);
        // When: its heartbeat repeats the same report
        let result = observe_presence(Some(&known), presence("idle")).unwrap();
        // Then: no fact is written for an unchanged heartbeat
        assert!(result.is_empty());
    }

    #[test]
    fn a_changed_report_records_the_next_status_change() {
        // Given: a live instance that already changed status twice
        let known = fleet(vec![live("idle", 2)]);
        // When: it reports that it is now busy
        let result = observe_presence(Some(&known), presence("busy")).unwrap();
        // Then: the change is numbered after the last one
        match result.events.first() {
            Some(FleetEvent::InstanceStatusReported(fact)) => {
                assert_eq!(fact.reported_status.as_str(), "busy");
                assert_eq!(fact.change_number, 3);
            }
            other => panic!("expected a status change, got {other:?}"),
        }
    }

    #[test]
    fn losing_an_instance_closes_the_session_it_had_open() {
        // Given: a live instance
        let instance = live("busy", 1);
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
}
