use crate::domain::actions::{Affordance, Availability};
use crate::domain::fleet::RunnerType;

pub const DISPATCH: &str = "dispatch";

impl RunnerType {
    pub fn can_dispatch(&self) -> Availability {
        if self.is_available() {
            Availability::Available
        } else {
            Availability::blocked("runner_type_unavailable")
        }
    }

    pub fn affordances(&self) -> Vec<Affordance> {
        vec![Affordance::new(DISPATCH, self.can_dispatch())]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::fleet::RunnerTypeState;
    use crate::domain::fleet::instance::RunnerInstance;
    use crate::domain::ids::{PresenceSessionId, RunnerTypeId};
    use crate::domain::keys::{InstanceKey, ReportedStatus, RunnerTypeKey, RunnerVersion};
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

    fn live() -> RunnerInstance {
        RunnerInstance::hydrate(
            InstanceKey::new("pod-7").unwrap(),
            PresenceSessionId::new(Uuid::now_v7()).unwrap(),
            RunnerVersion::new("1.4.2").unwrap(),
            ReportedStatus::new("idle").unwrap(),
            ts(1),
            ts(6),
            0,
        )
        .unwrap()
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
    fn dispatch_opens_as_soon_as_one_instance_is_live() {
        // Given: a runner type with one live instance
        let populated = runner_type(vec![live()]);
        // When/Then: dispatch is available
        assert_eq!(populated.can_dispatch(), Availability::Available);
    }
}
