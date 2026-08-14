use serde::{Deserialize, Serialize};

use crate::domain::ids::JobId;
use crate::domain::keys::{ProducerKey, RunnerTypeKey};
use crate::domain::references::KnownUser;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobOwner {
    Producer(ProducerKey),
    Runner {
        parent_job_id: JobId,
        runner_type: RunnerTypeKey,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Caller {
    Producer(ProducerKey),
    Runner {
        runner_type: RunnerTypeKey,
        executing_job_id: JobId,
    },
    Administrator(KnownUser),
}

impl JobOwner {
    pub fn authorizes(&self, caller: &Caller) -> bool {
        match (self, caller) {
            (Self::Producer(owner), Caller::Producer(claimed)) => owner == claimed,
            (
                Self::Runner {
                    parent_job_id,
                    runner_type,
                },
                Caller::Runner {
                    runner_type: claimed_type,
                    executing_job_id,
                },
            ) => parent_job_id == executing_job_id && runner_type == claimed_type,
            _ => false,
        }
    }

    pub fn parent_job_id(&self) -> Option<JobId> {
        match self {
            Self::Producer(_) => None,
            Self::Runner { parent_job_id, .. } => Some(*parent_job_id),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CancelRequester {
    Administrator(KnownUser),
    Owner(Caller),
    Cascade { originating_job_id: JobId },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::keys::DisplayName;
    use br_core_events::UserId;
    use uuid::Uuid;

    fn job_id() -> JobId {
        JobId::new(Uuid::now_v7()).unwrap()
    }

    #[test]
    fn a_producer_owned_job_answers_only_to_its_producer() {
        // Given: a root job owned by the producer that declared it
        let owner = JobOwner::Producer(ProducerKey::new("projects").unwrap());
        // When: another bounded context claims to be the owner
        // Then: only the declaring producer is authorized
        assert!(owner.authorizes(&Caller::Producer(ProducerKey::new("projects").unwrap())));
        assert!(!owner.authorizes(&Caller::Producer(ProducerKey::new("chat").unwrap())));
    }

    #[test]
    fn a_child_job_answers_only_to_the_runner_executing_its_parent() {
        // Given: a child job whose owner is the runner of the parent job
        let parent = job_id();
        let owner = JobOwner::Runner {
            parent_job_id: parent,
            runner_type: RunnerTypeKey::new("analyst").unwrap(),
        };
        // When: the runner executing that parent speaks
        let rightful = Caller::Runner {
            runner_type: RunnerTypeKey::new("analyst").unwrap(),
            executing_job_id: parent,
        };
        // Then: it is authorized, while the same runner type on another job is not
        assert!(owner.authorizes(&rightful));
        assert!(!owner.authorizes(&Caller::Runner {
            runner_type: RunnerTypeKey::new("analyst").unwrap(),
            executing_job_id: job_id(),
        }));
    }

    #[test]
    fn an_administrator_never_owns_a_job() {
        // Given: an owned job and a platform administrator
        let owner = JobOwner::Producer(ProducerKey::new("projects").unwrap());
        let admin = Caller::Administrator(
            KnownUser::new(
                UserId(Uuid::now_v7()),
                DisplayName::new("Operator").unwrap(),
            )
            .unwrap(),
        );
        // When/Then: administration is not ownership — finishing stays the owner's act
        assert!(!owner.authorizes(&admin));
    }
}
