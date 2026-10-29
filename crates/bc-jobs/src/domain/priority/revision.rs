use crate::error::JobsError;

/// Where a user's maximum stands among its changes: the number of changes of
/// it ever recorded, 0 for a user whose maximum never changed. Changes of one
/// user are recorded one after the other, so a later change always carries a
/// higher revision — what lets a live view tell a late or repeated change from
/// a new one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MaxPriorityRevision(u64);

impl MaxPriorityRevision {
    pub const NEVER_CHANGED: Self = Self(0);

    pub fn new(value: i64) -> Result<Self, JobsError> {
        u64::try_from(value)
            .map(Self)
            .map_err(|_| JobsError::OutOfRange {
                field: "max_priority_revision",
                value,
            })
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_revision_counts_changes_and_never_goes_negative() {
        // Given/When/Then: a recorded count is a revision, a negative one is corrupt input
        assert_eq!(MaxPriorityRevision::new(3).unwrap().get(), 3);
        assert!(MaxPriorityRevision::new(2).unwrap() > MaxPriorityRevision::NEVER_CHANGED);
        assert_eq!(
            MaxPriorityRevision::new(-1),
            Err(JobsError::OutOfRange {
                field: "max_priority_revision",
                value: -1
            })
        );
    }
}
