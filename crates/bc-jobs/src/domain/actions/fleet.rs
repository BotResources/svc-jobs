use crate::domain::actions::{Affordance, Availability};
use crate::domain::fleet::RunnerType;
use crate::domain::keys::RunnerTypeKey;
use crate::error::JobsError;

pub const DISPATCH: &str = "dispatch";

pub fn unregistered_affordances(runner_type: &RunnerTypeKey) -> Vec<Affordance> {
    vec![Affordance::new(
        DISPATCH,
        Availability::from_guard(Err(JobsError::RunnerTypeUnavailable {
            runner_type: runner_type.as_str().to_owned(),
        })),
    )]
}

impl RunnerType {
    pub fn guard_dispatch(&self) -> Result<(), JobsError> {
        if self.is_available() {
            Ok(())
        } else {
            Err(JobsError::RunnerTypeUnavailable {
                runner_type: self.key().as_str().to_owned(),
            })
        }
    }

    pub fn can_dispatch(&self) -> Availability {
        Availability::from_guard(self.guard_dispatch())
    }

    pub fn affordances(&self) -> Vec<Affordance> {
        vec![Affordance::new(DISPATCH, self.can_dispatch())]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::fleet::RunnerTypeState;
    use crate::domain::fleet::capacity::Capacity;
    use crate::domain::fleet::instance::{RunnerInstance, RunnerInstanceState};
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

    #[test]
    fn dispatch_is_blocked_while_no_instance_is_live() {
        // Given: a runner type whose instances have all disconnected
        let empty = runner_type(vec![]);
        // When: the backend answers whether work may go out
        // Then: the wait is explained by a code, not inferred by the client
        assert_eq!(
            empty.affordances().first().unwrap().reason_code(),
            Some("runner_type_unavailable")
        );
    }

    #[test]
    fn a_runner_type_never_seen_answers_the_same_dispatch_affordance_as_a_registered_idle_one() {
        // Given: a runner type key no instance has ever announced
        let never_seen = RunnerTypeKey::new("archivist").unwrap();
        // When: the backend answers whether work may go out
        let unregistered = unregistered_affordances(&never_seen);
        // Then: it is the domain's own DISPATCH verdict, identical in shape to a registered type's
        let registered = runner_type(vec![]);
        assert_eq!(unregistered.len(), 1);
        assert_eq!(unregistered.first().unwrap().action(), DISPATCH);
        assert_eq!(
            unregistered.first().unwrap().reason_code(),
            registered.affordances().first().unwrap().reason_code()
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
            draining.affordances().first().unwrap().reason_code(),
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
}
