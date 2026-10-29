use crate::domain::access::PrincipalFacts;
use crate::domain::actions::Affordance;
use crate::domain::ids::KnownUserId;
use crate::domain::policy::ServiceLimits;
use crate::domain::priority::level::Priority;
use crate::domain::priority::user_max::UserMaxPriority;

/// A user's maximum priority as an administrator sees it: the level that
/// applies, whether it is the service default rather than the user's own, and
/// the caller's affordances of setting and clearing it.
///
/// The one projection the administrator's read and its live changes share.
/// Every user has one — a user Jobs never heard of is on the default — so the
/// projection is never empty.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserMaxPriorityView {
    pub user_id: KnownUserId,
    pub max_priority: Priority,
    pub is_default: bool,
    pub affordances: Vec<Affordance>,
}

impl UserMaxPriority {
    pub fn view(&self, principal: &PrincipalFacts, limits: &ServiceLimits) -> UserMaxPriorityView {
        UserMaxPriorityView {
            user_id: self.user_id(),
            max_priority: self.applicable(limits),
            is_default: !self.is_set(),
            affordances: self.affordances(principal),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::access::{PlatformPrivileges, PrincipalKind};
    use crate::domain::actions::user::CLEAR_MAX_PRIORITY;
    use uuid::Uuid;

    fn level(value: i64) -> Priority {
        Priority::new(value).unwrap()
    }

    fn super_user() -> PrincipalFacts {
        PrincipalFacts::new(
            PrincipalKind::Human,
            true,
            PlatformPrivileges::PLATFORM_ORG_ADMIN,
        )
    }

    fn user(maximum: Option<i64>) -> UserMaxPriority {
        UserMaxPriority::new(
            KnownUserId::new(Uuid::now_v7()).unwrap(),
            maximum.map(level),
        )
    }

    fn clear_allowed(view: &UserMaxPriorityView) -> bool {
        view.affordances
            .iter()
            .find(|affordance| affordance.action() == CLEAR_MAX_PRIORITY)
            .is_some_and(Affordance::allowed)
    }

    #[test]
    fn a_user_with_a_maximum_of_their_own_reads_it_as_their_own() {
        // Given: a user capped at 5 under a default of 3
        let capped = user(Some(5));
        // When: projected for a super user
        let view = capped.view(&super_user(), &ServiceLimits::default());
        // Then: 5, not the default, and clearable
        assert_eq!(view.user_id, capped.user_id());
        assert_eq!(view.max_priority, level(5));
        assert!(!view.is_default);
        assert!(clear_allowed(&view));
    }

    #[test]
    fn a_user_without_one_reads_the_service_default_whatever_it_is() {
        // Given: a service whose default maximum is 2, and a user with none
        let limits = ServiceLimits::default().with_default_max_priority(level(2));
        // When: projected
        let view = user(None).view(&super_user(), &limits);
        // Then: the default applies and says so, with nothing to clear
        assert_eq!(view.max_priority, level(2));
        assert!(view.is_default);
        assert!(!clear_allowed(&view));
    }

    #[test]
    fn a_change_of_the_users_maximum_changes_the_view_and_an_unchanged_one_does_not() {
        // Given: the view of a user capped at 4
        let before = user(Some(4));
        let limits = ServiceLimits::default();
        let seen = before.view(&super_user(), &limits);
        // When: the same maximum is projected again, then a cleared one
        let same =
            UserMaxPriority::new(before.user_id(), Some(level(4))).view(&super_user(), &limits);
        let cleared = UserMaxPriority::new(before.user_id(), None).view(&super_user(), &limits);
        // Then: only the change yields another view — what a live delta is computed from
        assert_eq!(same, seen);
        assert_ne!(cleared, seen);
        assert!(cleared.is_default);
    }
}
