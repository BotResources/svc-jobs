use crate::commands::{CommandResult, CommandWarning, UserCommandResult};
use crate::domain::access::PrincipalFacts;
use crate::domain::priority::{Priority, UserMaxPriority};
use crate::domain::references::KnownUser;
use crate::error::JobsError;
use crate::event::user::{UserEvent, UserMaxPriorityCleared, UserMaxPrioritySet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetMaxPriority {
    pub max_priority: Priority,
    pub set_by: KnownUser,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClearMaxPriority {
    pub cleared_by: KnownUser,
}

pub fn set_max_priority(
    current: &UserMaxPriority,
    command: SetMaxPriority,
    principal: &PrincipalFacts,
) -> Result<UserCommandResult, JobsError> {
    current.guard_govern(principal)?;
    if current.maximum() == Some(command.max_priority) {
        return Ok(CommandResult::nothing_happened(
            CommandWarning::CommandAlreadyApplied {
                command: "set_max_priority",
            },
        ));
    }
    Ok(CommandResult::from_event(UserEvent::MaxPrioritySet(
        UserMaxPrioritySet {
            user_id: current.user_id(),
            max_priority: command.max_priority,
            set_by: command.set_by,
        },
    )))
}

pub fn clear_max_priority(
    current: &UserMaxPriority,
    command: ClearMaxPriority,
    principal: &PrincipalFacts,
) -> Result<UserCommandResult, JobsError> {
    current.guard_govern(principal)?;
    if !current.is_set() {
        return Ok(CommandResult::nothing_happened(
            CommandWarning::CommandAlreadyApplied {
                command: "clear_max_priority",
            },
        ));
    }
    Ok(CommandResult::from_event(UserEvent::MaxPriorityCleared(
        UserMaxPriorityCleared {
            user_id: current.user_id(),
            cleared_by: command.cleared_by,
        },
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::access::PrincipalKind;
    use crate::domain::ids::KnownUserId;
    use crate::fixtures::user;
    use uuid::Uuid;

    fn level(value: i64) -> Priority {
        Priority::new(value).unwrap()
    }

    fn super_user() -> PrincipalFacts {
        PrincipalFacts::new(PrincipalKind::Human, true, true)
    }

    fn member() -> PrincipalFacts {
        PrincipalFacts::new(PrincipalKind::Human, true, false)
    }

    fn capped(maximum: Option<i64>) -> UserMaxPriority {
        UserMaxPriority::new(
            KnownUserId::new(Uuid::now_v7()).unwrap(),
            maximum.map(level),
        )
    }

    fn set(value: i64) -> SetMaxPriority {
        SetMaxPriority {
            max_priority: level(value),
            set_by: user(),
        }
    }

    #[test]
    fn a_super_user_sets_a_users_maximum() {
        // Given: a user on the default maximum
        let current = capped(None);
        let command = set(4);
        let set_by = command.set_by.clone();
        // When: a super user sets their maximum to 4
        let result = set_max_priority(&current, command, &super_user()).unwrap();
        // Then: one fact names the user, the level and who set it
        assert_eq!(
            result.events,
            vec![UserEvent::MaxPrioritySet(UserMaxPrioritySet {
                user_id: current.user_id(),
                max_priority: level(4),
                set_by,
            })]
        );
    }

    #[test]
    fn setting_the_maximum_a_user_already_has_records_nothing() {
        // Given: a user capped at 4
        let current = capped(Some(4));
        // When/Then: setting 4 again records nothing
        assert!(
            set_max_priority(&current, set(4), &super_user())
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn a_member_may_neither_set_nor_clear_a_maximum() {
        // Given: a capped user and an ordinary member
        let current = capped(Some(2));
        // When/Then: both gestures are refused with the super-user code
        assert_eq!(
            set_max_priority(&current, set(5), &member()),
            Err(JobsError::NotSuperUser)
        );
        assert_eq!(
            clear_max_priority(&current, ClearMaxPriority { cleared_by: user() }, &member()),
            Err(JobsError::NotSuperUser)
        );
    }

    #[test]
    fn clearing_a_maximum_returns_the_user_to_the_default() {
        // Given: a user capped at 2
        let current = capped(Some(2));
        // When: a super user clears it
        let result = clear_max_priority(
            &current,
            ClearMaxPriority { cleared_by: user() },
            &super_user(),
        )
        .unwrap();
        // Then: the clearing is recorded
        assert!(matches!(
            result.events.as_slice(),
            [UserEvent::MaxPriorityCleared(fact)] if fact.user_id == current.user_id()
        ));
    }

    #[test]
    fn clearing_a_maximum_that_is_not_set_records_nothing() {
        // Given: a user on the default
        let current = capped(None);
        // When/Then: clearing records nothing
        assert!(
            clear_max_priority(
                &current,
                ClearMaxPriority { cleared_by: user() },
                &super_user()
            )
            .unwrap()
            .is_empty()
        );
    }
}
