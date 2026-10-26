use serde::{Deserialize, Serialize};

use crate::domain::priority::basis::PriorityBasis;
use crate::domain::priority::level::Priority;
use crate::error::JobsError;

/// The priority a Job requested, the one it waits at, and the level a super
/// user forced, if any. A forced level is the effective one until it is forced
/// again; no cap change touches it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobPriority {
    requested: Priority,
    effective: Priority,
    forced: Option<Priority>,
}

impl JobPriority {
    /// A root Job keeps what it requested and waits at the capped level. A
    /// child records its parent's effective level as both its request and its
    /// effective level: what it asked for itself is ignored.
    pub fn admitted(requested: Priority, basis: PriorityBasis) -> Self {
        let effective = basis.derive(requested);
        let requested = match basis {
            PriorityBasis::Capped { .. } => requested,
            PriorityBasis::InheritedFromParent { .. } => effective,
        };
        Self {
            requested,
            effective,
            forced: None,
        }
    }

    pub fn hydrate(
        requested: Priority,
        effective: Priority,
        forced: Option<Priority>,
    ) -> Result<Self, JobsError> {
        if forced.is_some_and(|forced| forced != effective) {
            return Err(JobsError::CorruptState {
                reason_code: "forced_priority_is_not_effective",
            });
        }
        Ok(Self {
            requested,
            effective,
            forced,
        })
    }

    pub fn requested(&self) -> Priority {
        self.requested
    }

    pub fn effective(&self) -> Priority {
        self.effective
    }

    pub fn forced(&self) -> Option<Priority> {
        self.forced
    }

    pub fn is_forced(&self) -> bool {
        self.forced.is_some()
    }

    /// The effective level a fresh derivation gives, or `None` when nothing
    /// would change: a forced level is never recalculated.
    pub fn recalculated(&self, basis: PriorityBasis) -> Option<Priority> {
        if self.is_forced() {
            return None;
        }
        let derived = basis.derive(self.requested);
        (derived != self.effective).then_some(derived)
    }

    pub fn is_forced_to(&self, level: Priority) -> bool {
        self.forced == Some(level)
    }
}

impl Default for JobPriority {
    fn default() -> Self {
        Self {
            requested: Priority::DEFAULT,
            effective: Priority::DEFAULT,
            forced: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn level(value: i64) -> Priority {
        Priority::new(value).unwrap()
    }

    #[test]
    fn an_admitted_priority_keeps_the_request_and_waits_at_the_derived_level() {
        // Given: a request of 5 under a cap of 3
        let priority = JobPriority::admitted(level(5), PriorityBasis::Capped { maximum: level(3) });
        // Then: the request is kept for the record, the job waits at 3, nothing is forced
        assert_eq!(priority.requested(), level(5));
        assert_eq!(priority.effective(), level(3));
        assert_eq!(priority.forced(), None);
    }

    #[test]
    fn a_child_records_its_parents_level_as_request_and_effective() {
        // Given: a child requesting 1 under a parent waiting at 5
        let priority = JobPriority::admitted(
            level(1),
            PriorityBasis::InheritedFromParent {
                parent_effective: level(5),
            },
        );
        // Then: it requested and waits at 5
        assert_eq!(priority.requested(), level(5));
        assert_eq!(priority.effective(), level(5));
    }

    #[test]
    fn a_stored_forced_level_that_is_not_the_effective_one_cannot_be_loaded() {
        // Given: a row forced at 5 but waiting at 3
        let result = JobPriority::hydrate(level(3), level(3), Some(level(5)));
        // Then: the contradiction is refused at load
        assert_eq!(
            result,
            Err(JobsError::CorruptState {
                reason_code: "forced_priority_is_not_effective"
            })
        );
    }

    #[test]
    fn a_new_cap_recalculates_an_unforced_priority() {
        // Given: a request of 5 admitted under a cap of 3
        let priority = JobPriority::admitted(level(5), PriorityBasis::Capped { maximum: level(3) });
        // When: the cap is raised to 4
        let recalculated = priority.recalculated(PriorityBasis::Capped { maximum: level(4) });
        // Then: the job now waits at 4
        assert_eq!(recalculated, Some(level(4)));
    }

    #[test]
    fn a_cap_that_derives_the_same_level_recalculates_nothing() {
        // Given: a request of 2 admitted under a cap of 3
        let priority = JobPriority::admitted(level(2), PriorityBasis::Capped { maximum: level(3) });
        // When: the cap moves to 5
        // Then: the request was never capped, so nothing changes
        assert_eq!(
            priority.recalculated(PriorityBasis::Capped { maximum: level(5) }),
            None
        );
    }

    #[test]
    fn a_forced_level_is_never_recalculated() {
        // Given: a job forced to 5
        let priority = JobPriority::hydrate(level(3), level(5), Some(level(5))).unwrap();
        // When: its user's cap is lowered to 1
        // Then: the forced level holds
        assert_eq!(
            priority.recalculated(PriorityBasis::Capped { maximum: level(1) }),
            None
        );
    }
}
