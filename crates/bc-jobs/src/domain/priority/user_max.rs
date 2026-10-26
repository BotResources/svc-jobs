use crate::domain::ids::KnownUserId;
use crate::domain::policy::ServiceLimits;
use crate::domain::priority::level::Priority;

/// The maximum priority Jobs stores for one KnownUser. A user without one is
/// capped by the service default.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UserMaxPriority {
    user_id: KnownUserId,
    maximum: Option<Priority>,
}

impl UserMaxPriority {
    pub fn new(user_id: KnownUserId, maximum: Option<Priority>) -> Self {
        Self { user_id, maximum }
    }

    pub fn user_id(&self) -> KnownUserId {
        self.user_id
    }

    /// The maximum set for this user, absent when none is.
    pub fn maximum(&self) -> Option<Priority> {
        self.maximum
    }

    /// The maximum that applies: the user's own, or the service default.
    pub fn applicable(&self, limits: &ServiceLimits) -> Priority {
        self.maximum
            .unwrap_or_else(|| limits.default_max_priority())
    }

    pub fn is_set(&self) -> bool {
        self.maximum.is_some()
    }
}
