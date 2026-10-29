//! The decisions on a user's maximum priority: setting it and clearing it.
//!
//! The commands enforce them and the administrator's read projects them as
//! affordances — one implementation each, so what the screen offers and what
//! the command accepts cannot disagree.

use crate::domain::access::PrincipalFacts;
use crate::domain::actions::{Affordance, Availability};
use crate::domain::priority::UserMaxPriority;
use crate::error::JobsError;

pub const SET_MAX_PRIORITY: &str = "set_max_priority";
pub const CLEAR_MAX_PRIORITY: &str = "clear_max_priority";

impl UserMaxPriority {
    /// Setting a user's maximum: super users only, whatever the user's current
    /// maximum. The level asked for is the command's input, validated apart.
    pub fn guard_set(&self, principal: &PrincipalFacts) -> Result<(), JobsError> {
        principal.guard_super_user()
    }

    /// Clearing a user's maximum: super users only, and the user has one of
    /// their own to clear.
    pub fn guard_clear(&self, principal: &PrincipalFacts) -> Result<(), JobsError> {
        principal.guard_super_user()?;
        if self.is_set() {
            Ok(())
        } else {
            Err(JobsError::MaxPriorityNotSet {
                user_id: self.user_id().as_uuid(),
            })
        }
    }

    pub fn can_set(&self, principal: &PrincipalFacts) -> Availability {
        Availability::from_guard(self.guard_set(principal))
    }

    pub fn can_clear(&self, principal: &PrincipalFacts) -> Availability {
        Availability::from_guard(self.guard_clear(principal))
    }

    pub fn affordances(&self, principal: &PrincipalFacts) -> Vec<Affordance> {
        vec![
            Affordance::new(SET_MAX_PRIORITY, self.can_set(principal)),
            Affordance::new(CLEAR_MAX_PRIORITY, self.can_clear(principal)),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::access::{PlatformRole, PrincipalKind};
    use crate::domain::ids::KnownUserId;
    use crate::domain::priority::Priority;
    use serde_json::json;
    use uuid::Uuid;

    fn human(role: PlatformRole) -> PrincipalFacts {
        PrincipalFacts::new(PrincipalKind::Human, true, role)
    }

    fn super_user() -> PrincipalFacts {
        human(PlatformRole::SuperUser)
    }

    fn user(maximum: Option<i64>) -> UserMaxPriority {
        UserMaxPriority::new(
            KnownUserId::new(Uuid::now_v7()).unwrap(),
            maximum.map(|level| Priority::new(level).unwrap()),
        )
    }

    fn decided(affordances: &[Affordance], action: &str) -> (bool, Option<String>) {
        let found = affordances
            .iter()
            .find(|affordance| affordance.action() == action)
            .unwrap_or_else(|| panic!("no '{action}' among {affordances:?}"));
        (found.allowed(), found.reason_code().map(str::to_owned))
    }

    #[test]
    fn a_super_user_may_set_any_users_maximum_and_clear_only_one_that_is_set() {
        // Given: user A with a maximum of 5 and user B with none
        let (a, b) = (user(Some(5)), user(None));
        // When: a super user's affordances are decided on both
        let (on_a, on_b) = (a.affordances(&super_user()), b.affordances(&super_user()));
        // Then: setting is allowed on both, clearing on A only
        assert_eq!(decided(&on_a, SET_MAX_PRIORITY), (true, None));
        assert_eq!(decided(&on_a, CLEAR_MAX_PRIORITY), (true, None));
        assert_eq!(decided(&on_b, SET_MAX_PRIORITY), (true, None));
        assert_eq!(
            decided(&on_b, CLEAR_MAX_PRIORITY),
            (false, Some("no_max_priority_set".to_owned()))
        );
    }

    #[test]
    fn the_refusal_to_clear_names_the_user_it_is_about() {
        // Given: a user on the default
        let b = user(None);
        // When/Then: the blocked affordance carries the user id as structured data
        let clear = b
            .affordances(&super_user())
            .into_iter()
            .find(|affordance| affordance.action() == CLEAR_MAX_PRIORITY)
            .unwrap();
        assert_eq!(
            clear.params(),
            Some(&json!({ "userId": b.user_id().as_uuid() }))
        );
    }

    #[test]
    fn a_member_is_refused_both_on_every_user_for_their_rights_not_the_users_state() {
        // Given: users A (maximum 5) and B (none), and an ordinary member
        for target in [user(Some(5)), user(None)] {
            // When: their affordances are decided
            let affordances = target.affordances(&human(PlatformRole::Member));
            // Then: setting and clearing are both refused on the super-user code
            for action in [SET_MAX_PRIORITY, CLEAR_MAX_PRIORITY] {
                assert_eq!(
                    decided(&affordances, action),
                    (false, Some("not_super_user".to_owned())),
                    "{action}"
                );
            }
        }
    }

    #[test]
    fn callers_without_any_platform_privilege_are_refused_on_the_super_user_code() {
        // Given: an ordinary member, a deactivated super user and a service account
        let callers = [
            human(PlatformRole::Member),
            PrincipalFacts::new(PrincipalKind::Human, false, PlatformRole::SuperUser),
            PrincipalFacts::new(PrincipalKind::Service, true, PlatformRole::SuperUser),
        ];
        for caller in callers {
            // When/Then: neither gesture is open on a user with a maximum to clear
            let target = user(Some(2));
            assert_eq!(target.guard_set(&caller), Err(JobsError::NotSuperUser));
            assert_eq!(target.guard_clear(&caller), Err(JobsError::NotSuperUser));
        }
    }
}
