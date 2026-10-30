use serde::{Deserialize, Serialize};

use crate::domain::ids::RunId;
use crate::domain::ownership::ActorRef;
use crate::error::JobsError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Requester {
    Service(Option<ActorRef>),
    Run(RunId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Caller {
    Service(ActorRef),
    Run { run_id: RunId, works_the_job: bool },
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ActsFor(Vec<RunId>);

impl ActsFor {
    pub fn new(runs: impl IntoIterator<Item = RunId>) -> Self {
        Self(runs.into_iter().collect())
    }

    pub fn includes(&self, run_id: RunId) -> bool {
        self.0.contains(&run_id)
    }
}

impl Requester {
    pub fn is_admitted(&self, caller: &Caller, acts_for: &ActsFor) -> bool {
        match (self, caller) {
            (Self::Service(Some(declared)), Caller::Service(claimed)) => declared == claimed,
            (Self::Run(requester), Caller::Run { run_id, .. }) => {
                requester == run_id || acts_for.includes(*requester)
            }
            _ => false,
        }
    }

    pub fn guard_resolves(&self, caller: &Caller, acts_for: &ActsFor) -> Result<(), JobsError> {
        if self.is_admitted(caller, acts_for) {
            Ok(())
        } else {
            Err(JobsError::NotRequester)
        }
    }

    pub fn guard_cancels(&self, caller: &Caller, acts_for: &ActsFor) -> Result<(), JobsError> {
        if self.is_admitted(caller, acts_for) {
            return Ok(());
        }
        match caller {
            Caller::Run {
                works_the_job: true,
                ..
            } => Ok(()),
            Caller::Run { .. } => Err(JobsError::RunnerNotWorkingTheJob),
            Caller::Service(_) => Err(JobsError::NotRequester),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixtures::run_id;
    use uuid::Uuid;

    fn actor() -> ActorRef {
        ActorRef::new(Uuid::now_v7())
    }

    #[test]
    fn a_service_requester_is_only_the_actor_that_declared_the_job() {
        let declared = actor();
        let requester = Requester::Service(Some(declared));
        assert!(requester.is_admitted(&Caller::Service(declared), &ActsFor::default()));
        assert!(!requester.is_admitted(&Caller::Service(actor()), &ActsFor::default()));
    }

    #[test]
    fn a_service_requester_without_a_known_actor_answers_to_nobody() {
        let requester = Requester::Service(None);
        assert!(!requester.is_admitted(&Caller::Service(actor()), &ActsFor::default()));
    }

    #[test]
    fn a_run_requester_is_the_run_that_created_the_job() {
        let run = run_id();
        let requester = Requester::Run(run);
        let caller = Caller::Run {
            run_id: run,
            works_the_job: false,
        };
        assert!(requester.is_admitted(&caller, &ActsFor::default()));
    }

    #[test]
    fn a_run_that_acts_for_the_requester_run_has_its_rights() {
        let origin = run_id();
        let restart = run_id();
        let caller = Caller::Run {
            run_id: restart,
            works_the_job: false,
        };
        let acts_for = ActsFor::new([origin]);
        assert!(Requester::Run(origin).is_admitted(&caller, &acts_for));
        assert!(!Requester::Run(origin).is_admitted(&caller, &ActsFor::default()));
    }

    #[test]
    fn a_service_never_stands_for_a_run_requester_nor_a_run_for_a_service() {
        let declared = actor();
        let run = run_id();
        let as_run = Caller::Run {
            run_id: run,
            works_the_job: true,
        };
        assert!(!Requester::Service(Some(declared)).is_admitted(&as_run, &ActsFor::default()));
        assert!(!Requester::Run(run).is_admitted(&Caller::Service(declared), &ActsFor::default()));
    }

    #[test]
    fn the_runner_working_the_job_may_cancel_it_but_not_finish_it() {
        let requester = Requester::Run(run_id());
        let worker = Caller::Run {
            run_id: run_id(),
            works_the_job: true,
        };
        assert_eq!(
            requester.guard_cancels(&worker, &ActsFor::default()),
            Ok(())
        );
        assert_eq!(
            requester.guard_resolves(&worker, &ActsFor::default()),
            Err(JobsError::NotRequester)
        );
    }

    #[test]
    fn a_runner_that_works_none_of_it_is_refused_a_cancel() {
        let requester = Requester::Run(run_id());
        let stranger = Caller::Run {
            run_id: run_id(),
            works_the_job: false,
        };
        assert_eq!(
            requester.guard_cancels(&stranger, &ActsFor::default()),
            Err(JobsError::RunnerNotWorkingTheJob)
        );
    }
}
