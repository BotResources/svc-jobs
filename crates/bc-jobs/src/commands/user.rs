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
    current.guard_set(principal)?;
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
    current.guard_clear(principal)?;
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
    use crate::domain::access::{PlatformRole, PrincipalKind};
    use crate::domain::ids::KnownUserId;
    use crate::fixtures::user;
    use uuid::Uuid;

    fn level(value: i64) -> Priority {
        Priority::new(value).unwrap()
    }

    fn super_user() -> PrincipalFacts {
        PrincipalFacts::new(PrincipalKind::Human, true, PlatformRole::SuperUser)
    }

    fn member() -> PrincipalFacts {
        PrincipalFacts::new(PrincipalKind::Human, true, PlatformRole::Member)
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
    fn clearing_a_maximum_that_is_not_set_is_refused_with_the_code_the_affordance_shows() {
        // Given: a user on the default, and the super user's affordance on them
        let current = capped(None);
        let affordance = current.can_clear(&super_user());
        // When: the super user clears anyway
        let result = clear_max_priority(
            &current,
            ClearMaxPriority { cleared_by: user() },
            &super_user(),
        );
        // Then: the command refuses on the very code the affordance announced
        assert_eq!(
            result,
            Err(JobsError::MaxPriorityNotSet {
                user_id: current.user_id().as_uuid()
            })
        );
        assert_eq!(affordance.reason_code(), Some("no_max_priority_set"));
    }

    #[test]
    fn each_command_is_accepted_exactly_when_its_affordance_is_allowed() {
        // Given: users with and without a maximum, and callers with and without the right
        let deactivated = PrincipalFacts::new(PrincipalKind::Human, false, PlatformRole::SuperUser);
        for current in [capped(Some(2)), capped(None)] {
            for principal in [super_user(), member(), deactivated] {
                // When: the affordance is decided and the command attempted
                let set = set_max_priority(&current, set(5), &principal);
                let clear = clear_max_priority(
                    &current,
                    ClearMaxPriority { cleared_by: user() },
                    &principal,
                );
                // Then: acceptance and refusal follow the one decision, refusal code included
                for (availability, outcome) in [
                    (current.can_set(&principal), set.map(|_| ())),
                    (current.can_clear(&principal), clear.map(|_| ())),
                ] {
                    match outcome {
                        Ok(()) => assert!(availability.is_available()),
                        Err(refusal) => {
                            assert_eq!(availability.reason_code(), Some(refusal.code()))
                        }
                    }
                }
            }
        }
    }
}
