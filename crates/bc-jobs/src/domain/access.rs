//! Who is asking: the trusted facts of the principal behind a request, and the
//! decisions every surface takes from them.
//!
//! The facts come from the Passport the trusted frontier injects; this module
//! never sees the Passport itself. Platform administrators read and operate
//! the ledger, users' maximum priorities included. Super users additionally
//! govern priorities. Any active human may read their own maximum priority —
//! and nothing else.

use crate::error::JobsError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrincipalKind {
    Human,
    Service,
}

/// The platform privileges a principal holds. Two facts, decided apart: the
/// frontier maps both from what the Passport carries, so the two populations
/// can part without touching any decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlatformPrivileges {
    pub platform_administrator: bool,
    pub super_user: bool,
}

impl PlatformPrivileges {
    pub const NONE: Self = Self {
        platform_administrator: false,
        super_user: false,
    };

    /// An admin of the platform org: a platform administrator and a super user.
    pub const PLATFORM_ORG_ADMIN: Self = Self {
        platform_administrator: true,
        super_user: true,
    };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PrincipalFacts {
    kind: PrincipalKind,
    is_active: bool,
    privileges: PlatformPrivileges,
}

impl PrincipalFacts {
    pub fn new(kind: PrincipalKind, is_active: bool, privileges: PlatformPrivileges) -> Self {
        Self {
            kind,
            is_active,
            privileges,
        }
    }

    fn is_active_human(&self) -> bool {
        self.kind == PrincipalKind::Human && self.is_active
    }

    /// Every administrator gesture and every read of the ledger, users'
    /// maximum priorities included.
    pub fn guard_platform_administrator(&self) -> Result<(), JobsError> {
        if self.is_active_human() && self.privileges.platform_administrator {
            Ok(())
        } else {
            Err(JobsError::NotPlatformAdministrator)
        }
    }

    /// Setting or clearing a user's maximum priority, and forcing a Job's
    /// priority: reserved to the admins of the platform org.
    pub fn guard_super_user(&self) -> Result<(), JobsError> {
        if self.is_active_human() && self.privileges.super_user {
            Ok(())
        } else {
            Err(JobsError::NotSuperUser)
        }
    }

    /// Reading one's own maximum priority: any active human, never a service
    /// account.
    pub fn guard_reads_own_max_priority(&self) -> Result<(), JobsError> {
        if self.is_active_human() {
            Ok(())
        } else {
            Err(JobsError::NotActiveHuman)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn privileges(platform_administrator: bool, super_user: bool) -> PlatformPrivileges {
        PlatformPrivileges {
            platform_administrator,
            super_user,
        }
    }

    fn human(is_active: bool, is_super_admin: bool) -> PrincipalFacts {
        PrincipalFacts::new(
            PrincipalKind::Human,
            is_active,
            privileges(is_super_admin, is_super_admin),
        )
    }

    fn service(is_super_admin: bool) -> PrincipalFacts {
        PrincipalFacts::new(
            PrincipalKind::Service,
            true,
            privileges(is_super_admin, is_super_admin),
        )
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

    #[test]
    fn a_platform_administrator_who_is_not_a_super_user_administers_but_governs_no_priority() {
        // Given: an active human holding the administrator privilege alone
        let administrator =
            PrincipalFacts::new(PrincipalKind::Human, true, privileges(true, false));
        // When/Then: the ledger is open, priority governance is not
        assert_eq!(administrator.guard_platform_administrator(), Ok(()));
        assert_eq!(
            administrator.guard_super_user(),
            Err(JobsError::NotSuperUser)
        );
    }

    #[test]
    fn the_super_user_privilege_alone_opens_no_administrator_read() {
        // Given: an active human holding the super-user privilege alone
        let governor = PrincipalFacts::new(PrincipalKind::Human, true, privileges(false, true));
        // When/Then: each decision reads its own fact
        assert_eq!(governor.guard_super_user(), Ok(()));
        assert_eq!(
            governor.guard_platform_administrator(),
            Err(JobsError::NotPlatformAdministrator)
        );
    }
}
