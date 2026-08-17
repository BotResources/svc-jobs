use crate::commands::{CommandResult, CommandWarning, FleetCommandResult};
use crate::domain::fleet::RunnerType;
use crate::domain::fleet::capacity::Capacity;
use crate::domain::fleet::instance::RunnerInstance;
use crate::domain::fleet::status::ReportedStatus;
use crate::domain::ids::{PresenceSessionId, RunnerTypeId};
use crate::domain::keys::{InstanceKey, ReasonCode, RunnerTypeKey, RunnerVersion};
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
    pub capacity: Capacity,
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
            connected(
                command.runner_type_id,
                command.runner_type.clone(),
                &command,
            ),
        ]));
    };
    let Some(live) = runner_type.instance(&command.instance_key) else {
        return Ok(CommandResult::from_event(connected(
            runner_type.id(),
            runner_type.key().clone(),
            &command,
        )));
    };
    if live.reports_the_same_as(&command.version, command.reported_status, command.capacity) {
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
            capacity: command.capacity,
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

fn connected(
    runner_type_id: RunnerTypeId,
    runner_type: RunnerTypeKey,
    command: &ObservePresence,
) -> FleetEvent {
    FleetEvent::InstanceConnected(InstanceConnected {
        runner_type_id,
        runner_type,
        instance_key: command.instance_key.clone(),
        session_id: command.session_id,
        version: command.version.clone(),
        reported_status: command.reported_status,
        capacity: command.capacity,
    })
}

#[cfg(test)]
mod tests;
