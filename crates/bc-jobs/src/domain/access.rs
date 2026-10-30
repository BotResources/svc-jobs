use crate::error::JobsError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrincipalKind {
    Human,
    Service,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum PlatformRole {
    Member,
    SuperUser,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrincipalFacts {
    kind: PrincipalKind,
    is_active: bool,
    role: PlatformRole,
}

impl PrincipalFacts {
    pub fn new(kind: PrincipalKind, is_active: bool, role: PlatformRole) -> Self {
        Self {
            kind,
            is_active,
            role,
        }
    }

    fn is_active_human_holding(&self, role: PlatformRole) -> bool {
        self.kind == PrincipalKind::Human && self.is_active && self.role >= role
    }

    pub fn guard_platform_administrator(&self) -> Result<(), JobsError> {
        if self.is_active_human_holding(PlatformRole::SuperUser) {
            Ok(())
        } else {
            Err(JobsError::NotPlatformAdministrator)
        }
    }

    pub fn guard_super_user(&self) -> Result<(), JobsError> {
        if self.is_active_human_holding(PlatformRole::SuperUser) {
            Ok(())
        } else {
            Err(JobsError::NotSuperUser)
        }
    }

    pub fn guard_holds_runner_scope(&self, scope_held: bool) -> Result<(), JobsError> {
        if self.kind == PrincipalKind::Service && self.is_active && scope_held {
            Ok(())
        } else {
            Err(JobsError::NotRunnerService)
        }
    }

    pub fn guard_reads_own_max_priority(&self) -> Result<(), JobsError> {
        if self.is_active_human_holding(PlatformRole::Member) {
            Ok(())
        } else {
            Err(JobsError::NotActiveHuman)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn role(is_super_admin: bool) -> PlatformRole {
        if is_super_admin {
            PlatformRole::SuperUser
        } else {
            PlatformRole::Member
        }
    }

    fn human(is_active: bool, is_super_admin: bool) -> PrincipalFacts {
        PrincipalFacts::new(PrincipalKind::Human, is_active, role(is_super_admin))
    }

    fn service(is_super_admin: bool) -> PrincipalFacts {
        PrincipalFacts::new(PrincipalKind::Service, true, role(is_super_admin))
    }

    #[test]
    fn an_active_super_administrator_administers_and_governs_priorities() {
        let admin = human(true, true);
        assert_eq!(admin.guard_platform_administrator(), Ok(()));
        assert_eq!(admin.guard_super_user(), Ok(()));
        assert_eq!(admin.guard_reads_own_max_priority(), Ok(()));
    }

    #[test]
    fn an_ordinary_human_reads_only_their_own_maximum() {
        let member = human(true, false);
        assert_eq!(member.guard_reads_own_max_priority(), Ok(()));
        assert_eq!(
            member.guard_platform_administrator(),
            Err(JobsError::NotPlatformAdministrator)
        );
        assert_eq!(member.guard_super_user(), Err(JobsError::NotSuperUser));
    }

    #[test]
    fn a_service_account_has_no_surface_at_all_whatever_it_claims() {
        for caller in [service(false), service(true)] {
            assert_eq!(
                caller.guard_platform_administrator(),
                Err(JobsError::NotPlatformAdministrator)
            );
            assert_eq!(caller.guard_super_user(), Err(JobsError::NotSuperUser));
            assert_eq!(
                caller.guard_reads_own_max_priority(),
                Err(JobsError::NotActiveHuman)
            );
        }
    }

    #[test]
    fn only_an_active_service_account_holding_the_runner_scope_is_a_runner() {
        let active = PrincipalFacts::new(PrincipalKind::Service, true, PlatformRole::Member);
        let inactive = PrincipalFacts::new(PrincipalKind::Service, false, PlatformRole::Member);
        assert_eq!(active.guard_holds_runner_scope(true), Ok(()));
        assert_eq!(
            active.guard_holds_runner_scope(false),
            Err(JobsError::NotRunnerService)
        );
        assert_eq!(
            inactive.guard_holds_runner_scope(true),
            Err(JobsError::NotRunnerService)
        );
        assert_eq!(
            human(true, true).guard_holds_runner_scope(true),
            Err(JobsError::NotRunnerService)
        );
    }

    #[test]
    fn a_deactivated_human_is_refused_everywhere_even_as_an_administrator() {
        let deactivated = human(false, true);
        assert!(deactivated.guard_platform_administrator().is_err());
        assert!(deactivated.guard_super_user().is_err());
        assert!(deactivated.guard_reads_own_max_priority().is_err());
    }
}
