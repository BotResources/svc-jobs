use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::domain::ids::JobId;
use crate::domain::keys::{ProducerKey, RunnerTypeKey};
use crate::domain::references::KnownUser;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
pub enum CancelRequester {
    Administrator(KnownUser),
    Owner(DeclarationClaim),
    Cascade { originating_job_id: JobId },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn actor() -> ActorRef {
        ActorRef::new(Uuid::now_v7())
    }

    #[test]
    fn a_job_answers_to_the_actor_that_declared_it() {
        // Given: the actor recorded on the job's declaration
        let owner = actor();
        // When: that same actor claims the job
        let claim = DeclarationClaim::new(Some(owner), owner);
        // Then: it owns it
        assert_eq!(claim.guard_owns_the_job(), Ok(()));
    }

    #[test]
    fn another_actor_of_the_same_bounded_context_is_not_the_owner() {
        // Given: a job declared by one actor
        let declared_by = actor();
        // When: a different actor claims it
        let claim = DeclarationClaim::new(Some(declared_by), actor());
        // Then: ownership is the declaring actor's, never a neighbour's
        assert_eq!(claim.guard_owns_the_job(), Err(JobsError::NotOwner));
    }

    #[test]
    fn a_job_whose_declaration_carries_no_actor_answers_to_nobody() {
        // Given: no declaring actor on record
        // When: anyone claims the job
        let claim = DeclarationClaim::new(None, actor());
        // Then: an unattributable declaration confers ownership on no one
        assert_eq!(claim.guard_owns_the_job(), Err(JobsError::NotOwner));
    }
}
