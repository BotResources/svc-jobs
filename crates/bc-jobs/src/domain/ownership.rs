use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::domain::ids::JobId;
use crate::domain::keys::{ProducerKey, RunnerTypeKey};
use crate::domain::references::KnownUser;
use crate::domain::requester::{ActsFor, Caller, Requester};
use crate::error::JobsError;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobOwner {
    Producer(ProducerKey),
    Runner {
        parent_job_id: JobId,
        runner_type: RunnerTypeKey,
    },
}

impl JobOwner {
    pub fn parent_job_id(&self) -> Option<JobId> {
        match self {
            Self::Producer(_) => None,
            Self::Runner { parent_job_id, .. } => Some(*parent_job_id),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActorRef(Uuid);

impl ActorRef {
    pub fn new(id: Uuid) -> Self {
        Self(id)
    }

    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeclarationClaim {
    declared_by: Option<ActorRef>,
    claimed_by: ActorRef,
}

impl DeclarationClaim {
    pub fn new(declared_by: Option<ActorRef>, claimed_by: ActorRef) -> Self {
        Self {
            declared_by,
            claimed_by,
        }
    }

    pub fn guard_owns_the_job(&self) -> Result<(), JobsError> {
        if self.declared_by == Some(self.claimed_by) {
            Ok(())
        } else {
            Err(JobsError::NotOwner)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolutionRequester {
    LegacyOwner(DeclarationClaim),
    Declarant,
    Caller { caller: Caller, acts_for: ActsFor },
}

impl ResolutionRequester {
    pub fn guard_may_resolve(&self, job_requester: &Requester) -> Result<(), JobsError> {
        match self {
            Self::LegacyOwner(claim) => claim.guard_owns_the_job(),
            Self::Declarant => guard_service_requested(job_requester),
            Self::Caller { caller, acts_for } => job_requester.guard_resolves(caller, acts_for),
        }
    }
}

fn guard_service_requested(job_requester: &Requester) -> Result<(), JobsError> {
    match job_requester {
        Requester::Service(_) => Ok(()),
        Requester::Run(_) => Err(JobsError::NotRequester),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CancelRequester {
    Administrator(KnownUser),
    Owner(DeclarationClaim),
    Declarant,
    Cascade { originating_job_id: JobId },
    Caller { caller: Caller, acts_for: ActsFor },
}

impl CancelRequester {
    pub fn guard_may_cancel(&self, job_requester: &Requester) -> Result<(), JobsError> {
        match self {
            Self::Owner(claim) => claim.guard_owns_the_job(),
            Self::Declarant => guard_service_requested(job_requester),
            Self::Caller { caller, acts_for } => job_requester.guard_cancels(caller, acts_for),
            Self::Administrator(_) | Self::Cascade { .. } => Ok(()),
        }
    }

    pub fn may_cancel_a_terminal_job(&self) -> bool {
        matches!(self, Self::Caller { .. } | Self::Cascade { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn actor() -> ActorRef {
        ActorRef::new(Uuid::now_v7())
    }

    #[test]
    fn a_job_answers_to_the_actor_that_declared_it() {
        let owner = actor();
        let claim = DeclarationClaim::new(Some(owner), owner);
        assert_eq!(claim.guard_owns_the_job(), Ok(()));
    }

    #[test]
    fn another_actor_of_the_same_bounded_context_is_not_the_owner() {
        let declared_by = actor();
        let claim = DeclarationClaim::new(Some(declared_by), actor());
        assert_eq!(claim.guard_owns_the_job(), Err(JobsError::NotOwner));
    }

    #[test]
    fn a_job_whose_declaration_carries_no_actor_answers_to_nobody() {
        let claim = DeclarationClaim::new(None, actor());
        assert_eq!(claim.guard_owns_the_job(), Err(JobsError::NotOwner));
    }

    #[test]
    fn a_legacy_resolution_still_requires_the_declaring_actor() {
        let requester =
            ResolutionRequester::LegacyOwner(DeclarationClaim::new(Some(actor()), actor()));
        assert_eq!(
            requester.guard_may_resolve(&Requester::Service(None)),
            Err(JobsError::NotOwner)
        );
    }

    #[test]
    fn a_declarant_admitted_by_the_fabric_is_checked_only_against_the_lifecycle() {
        let requester = ResolutionRequester::Declarant;
        assert_eq!(
            requester.guard_may_resolve(&Requester::Service(None)),
            Ok(())
        );
    }

    #[test]
    fn a_legacy_cancellation_still_requires_the_declaring_actor() {
        let requester = CancelRequester::Owner(DeclarationClaim::new(Some(actor()), actor()));
        assert_eq!(
            requester.guard_may_cancel(&Requester::Service(None)),
            Err(JobsError::NotOwner)
        );
    }

    #[test]
    fn a_declarant_admitted_by_the_fabric_may_request_cancellation() {
        let requester = CancelRequester::Declarant;
        assert_eq!(
            requester.guard_may_cancel(&Requester::Service(None)),
            Ok(())
        );
    }
}
