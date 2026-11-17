use uuid::Uuid;

use crate::domain::references::KnownUser;
use crate::error::JobsError;

/// A Job runs for a user when that user is its `triggered_by`.
pub fn guard_runs_for(triggered_by: Option<&KnownUser>, user_id: Uuid) -> Result<(), JobsError> {
    match triggered_by {
        Some(user) if user.id().0 == user_id => Ok(()),
        _ => Err(JobsError::NotJobUser),
    }
}

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
    user_id: Option<Uuid>,
}

impl PrincipalFacts {
    pub fn new(kind: PrincipalKind, is_active: bool, role: PlatformRole) -> Self {
        Self {
            kind,
            is_active,
            role,
            user_id: None,
        }
    }

    /// The user the caller is: the Jobs that run for that user are theirs.
    pub fn acting_as(mut self, user_id: Uuid) -> Self {
        self.user_id = Some(user_id);
        self
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

    /// The user surface reaches a Job only when it runs for the caller: an
    /// active human whose id is the Job's `triggered_by`.
    pub fn guard_job_runs_for_caller(
        &self,
        triggered_by: Option<&KnownUser>,
    ) -> Result<(), JobsError> {
        if !self.is_active_human_holding(PlatformRole::Member) {
            return Err(JobsError::NotActiveHuman);
        }
        match self.user_id {
            Some(caller) => guard_runs_for(triggered_by, caller),
            None => Err(JobsError::NotJobUser),
        }
    }

    /// A platform administrator cancels any Job; any other active human only a
    /// Job that runs for them.
    pub fn guard_cancels_job(&self, triggered_by: Option<&KnownUser>) -> Result<(), JobsError> {
        if self.guard_platform_administrator().is_ok() {
            return Ok(());
        }
        self.guard_job_runs_for_caller(triggered_by)
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

    fn someone(id: Uuid) -> KnownUser {
        KnownUser::id_only(br_core_events::UserId(id)).unwrap()
    }

    #[test]
    fn an_ordinary_human_reaches_only_the_jobs_that_run_for_them() {
        let alice = Uuid::now_v7();
        let member = human(true, false).acting_as(alice);
        assert_eq!(
            member.guard_job_runs_for_caller(Some(&someone(alice))),
            Ok(())
        );
        assert_eq!(member.guard_cancels_job(Some(&someone(alice))), Ok(()));
        for other in [Some(someone(Uuid::now_v7())), None] {
            assert_eq!(
                member.guard_job_runs_for_caller(other.as_ref()),
                Err(JobsError::NotJobUser)
            );
            assert_eq!(
                member.guard_cancels_job(other.as_ref()),
                Err(JobsError::NotJobUser)
            );
        }
    }

    #[test]
    fn an_administrator_cancels_any_job_but_follows_only_their_own_on_the_user_surface() {
        let admin = human(true, true).acting_as(Uuid::now_v7());
        let bob = someone(Uuid::now_v7());
        assert_eq!(admin.guard_cancels_job(Some(&bob)), Ok(()));
        assert_eq!(admin.guard_cancels_job(None), Ok(()));
        assert_eq!(
            admin.guard_job_runs_for_caller(Some(&bob)),
            Err(JobsError::NotJobUser)
        );
    }

    #[test]
    fn a_deactivated_human_or_a_service_never_reaches_a_job_that_runs_for_them() {
        let id = Uuid::now_v7();
        let deactivated = human(false, false).acting_as(id);
        let machine = service(false).acting_as(id);
        for caller in [deactivated, machine] {
            assert_eq!(
                caller.guard_job_runs_for_caller(Some(&someone(id))),
                Err(JobsError::NotActiveHuman)
            );
            assert_eq!(
                caller.guard_cancels_job(Some(&someone(id))),
                Err(JobsError::NotActiveHuman)
            );
        }
    }

    #[test]
    fn a_deactivated_human_is_refused_everywhere_even_as_an_administrator() {
        let deactivated = human(false, true);
        assert!(deactivated.guard_platform_administrator().is_err());
        assert!(deactivated.guard_super_user().is_err());
        assert!(deactivated.guard_reads_own_max_priority().is_err());
    }
}
