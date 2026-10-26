use crate::domain::job::Job;
use crate::domain::job::parenting::ParentContext;
use crate::domain::policy::ServiceLimits;
use crate::domain::priority::level::Priority;
use crate::domain::priority::user_max::UserMaxPriority;
use crate::error::JobsError;

/// What a Job's effective priority derives from, besides its own request.
///
/// A root Job is capped by the maximum of the user it runs for — or by the
/// service default when it runs for nobody. A child Job takes the effective
/// priority of its parent, whatever it requested.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PriorityBasis {
    Capped { maximum: Priority },
    InheritedFromParent { parent_effective: Priority },
}

impl PriorityBasis {
    /// The basis a Job stands on: its parent's effective priority when it has a
    /// parent, the cap of the user it runs for otherwise. `user_maximum` is the
    /// stored maximum of that user, absent when the user has none or the Job
    /// runs for nobody.
    pub fn of(
        parent: Option<&Job>,
        user_maximum: Option<&UserMaxPriority>,
        limits: &ServiceLimits,
    ) -> Self {
        match parent {
            Some(parent) => Self::InheritedFromParent {
                parent_effective: parent.priority().effective(),
            },
            None => Self::Capped {
                maximum: user_maximum
                    .and_then(UserMaxPriority::maximum)
                    .unwrap_or_else(|| limits.default_max_priority()),
            },
        }
    }

    /// The basis of an existing Job, judged with its parent loaded alongside it.
    pub fn of_job(
        job: &Job,
        parent: ParentContext<'_>,
        user_maximum: Option<&UserMaxPriority>,
        limits: &ServiceLimits,
    ) -> Result<Self, JobsError> {
        Ok(Self::of(
            parent.resolve(job.parent_job_id())?,
            user_maximum,
            limits,
        ))
    }

    pub fn derive(self, requested: Priority) -> Priority {
        match self {
            Self::Capped { maximum } => requested.capped_by(maximum),
            Self::InheritedFromParent { parent_effective } => parent_effective,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::ids::KnownUserId;
    use crate::fixtures::{JobBuilder, job_id};
    use uuid::Uuid;

    fn level(value: i64) -> Priority {
        Priority::new(value).unwrap()
    }

    fn capped_user(maximum: i64) -> UserMaxPriority {
        UserMaxPriority::new(
            KnownUserId::new(Uuid::now_v7()).unwrap(),
            Some(level(maximum)),
        )
    }

    #[test]
    fn a_root_job_is_capped_by_the_maximum_of_its_user() {
        // Given: a user capped at 3
        let user = capped_user(3);
        // When: a root job of that user requests 5
        let basis = PriorityBasis::of(None, Some(&user), &ServiceLimits::default());
        // Then: its effective priority is 3
        assert_eq!(basis.derive(level(5)), level(3));
    }

    #[test]
    fn a_user_without_a_maximum_is_capped_by_the_service_default() {
        // Given: a service whose default maximum is 4, and a user with no maximum of its own
        let limits = ServiceLimits::default().with_default_max_priority(level(4));
        let user = UserMaxPriority::new(KnownUserId::new(Uuid::now_v7()).unwrap(), None);
        // When: that user's job requests 5
        let basis = PriorityBasis::of(None, Some(&user), &limits);
        // Then: the service default caps it
        assert_eq!(basis.derive(level(5)), level(4));
    }

    #[test]
    fn a_job_that_runs_for_nobody_is_capped_by_the_service_default() {
        // Given: a service whose default maximum is 2
        let limits = ServiceLimits::default().with_default_max_priority(level(2));
        // When: an autonomous job requests 5
        let basis = PriorityBasis::of(None, None, &limits);
        // Then: the default caps it — no triggered_by is not a way around the cap
        assert_eq!(basis.derive(level(5)), level(2));
    }

    #[test]
    fn a_child_takes_its_parents_effective_priority_whatever_it_requested() {
        // Given: a parent waiting at effective priority 5, and a user capped at 1
        let parent = JobBuilder::new()
            .with_priority(level(5), level(5), None)
            .build();
        let user = capped_user(1);
        // When: a child requesting 1 is admitted under it
        let basis = PriorityBasis::of(Some(&parent), Some(&user), &ServiceLimits::default());
        // Then: it waits at its parent's level, neither its request nor its user's cap
        assert_eq!(basis.derive(level(1)), level(5));
    }

    #[test]
    fn the_basis_of_a_child_is_never_judged_without_its_parent() {
        // Given: a child whose parent was not loaded
        let child = JobBuilder::new().with_parent(job_id()).build();
        // When: its basis is asked for
        let result =
            PriorityBasis::of_job(&child, ParentContext::Root, None, &ServiceLimits::default());
        // Then: the wiring gap fails loud instead of capping the child like a root
        assert_eq!(
            result,
            Err(JobsError::CorruptState {
                reason_code: "parent_job_not_loaded"
            })
        );
    }
}
