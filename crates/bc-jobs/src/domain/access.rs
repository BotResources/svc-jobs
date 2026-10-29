//! Who is asking: the trusted facts of the principal behind a request, and the
//! decisions every surface takes from them.
//!
//! The facts come from the Passport the trusted frontier injects; this module
//! never sees the Passport itself. Super users — the admins of the platform
//! org, who are the platform administrators — read and operate the ledger and
//! govern priorities. Any active human may read their own maximum priority —
//! and nothing else.

use crate::error::JobsError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrincipalKind {
    Human,
    Service,
}

/// The role a principal holds on the platform. A platform administrator and a
/// super user are one population: the admins of the platform org. The decisions
/// that ask for either stay distinct, each refusing under its own code.
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

    /// Every administrator gesture and every read of the ledger, users'
    /// maximum priorities included.
    pub fn guard_platform_administrator(&self) -> Result<(), JobsError> {
        if self.is_active_human_holding(PlatformRole::SuperUser) {
            Ok(())
        } else {
            Err(JobsError::NotPlatformAdministrator)
        }
    }

    /// Setting or clearing a user's maximum priority, and forcing a Job's
    /// priority: reserved to the admins of the platform org.
    pub fn guard_super_user(&self) -> Result<(), JobsError> {
        if self.is_active_human_holding(PlatformRole::SuperUser) {
            Ok(())
        } else {
            Err(JobsError::NotSuperUser)
        }
    }

    /// Reading one's own maximum priority: any active human, never a service
    /// account.
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
        // Given: an active human admin of the platform org
        let admin = human(true, true);
        // When/Then: every gesture is open
        assert_eq!(admin.guard_platform_administrator(), Ok(()));
        assert_eq!(admin.guard_super_user(), Ok(()));
        assert_eq!(admin.guard_reads_own_max_priority(), Ok(()));
    }

    #[test]
    fn an_ordinary_human_reads_only_their_own_maximum() {
        // Given: an active human without platform privilege
        let member = human(true, false);
        // When/Then: the user surface is open, the administrator and super-user ones are not
        assert_eq!(member.guard_reads_own_max_priority(), Ok(()));
        assert_eq!(
            member.guard_platform_administrator(),
            Err(JobsError::NotPlatformAdministrator)
        );
        assert_eq!(member.guard_super_user(), Err(JobsError::NotSuperUser));
    }

    #[test]
    fn a_service_account_has_no_surface_at_all_whatever_it_claims() {
        // Given: service accounts, one claiming the super-admin flag
        for caller in [service(false), service(true)] {
            // When/Then: none of the three decisions admits it
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
    fn a_deactivated_human_is_refused_everywhere_even_as_an_administrator() {
        // Given: a deactivated super administrator
        let deactivated = human(false, true);
        // When/Then: nothing is open
        assert!(deactivated.guard_platform_administrator().is_err());
        assert!(deactivated.guard_super_user().is_err());
        assert!(deactivated.guard_reads_own_max_priority().is_err());
    }
}
