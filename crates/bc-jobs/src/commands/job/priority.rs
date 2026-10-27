use crate::commands::{CommandResult, CommandWarning, JobCommandResult};
use crate::domain::access::PrincipalFacts;
use crate::domain::ids::{JobId, KnownUserId};
use crate::domain::job::Job;
use crate::domain::priority::{Priority, PriorityBasis};
use crate::domain::references::KnownUser;
use crate::error::JobsError;
use crate::event::job::JobEvent;
use crate::event::job_facts::{JobPriorityForced, JobPriorityRecalculated};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForcePriority {
    pub priority: Priority,
    pub forced_by: KnownUser,
}

impl Job {
    /// A super user forces this Job to a level, above any cap. Forcing the
    /// level it already holds records nothing.
    pub fn force_priority(
        &self,
        command: ForcePriority,
        principal: &PrincipalFacts,
    ) -> Result<JobCommandResult, JobsError> {
        self.guard_force_priority(principal)?;
        Ok(self.forced(command.priority, command.forced_by, None))
    }

    /// The same level reaching a waiting descendant of the Job a super user
    /// forced, whether it was forced before or not. A descendant that stopped
    /// waiting since the cascade was decided, or already stands forced at that
    /// level, is left alone.
    pub fn force_priority_as_descendant(
        &self,
        command: ForcePriority,
        originating_job_id: JobId,
    ) -> JobCommandResult {
        if !self.is_waiting() || self.priority().is_forced_to(command.priority) {
            return CommandResult::new(vec![]);
        }
        self.forced(
            command.priority,
            command.forced_by,
            Some(originating_job_id),
        )
    }

    fn forced(
        &self,
        priority: Priority,
        forced_by: KnownUser,
        originating_job_id: Option<JobId>,
    ) -> JobCommandResult {
        if self.priority().is_forced_to(priority) {
            return CommandResult::nothing_happened(CommandWarning::CommandAlreadyApplied {
                command: "force_priority",
            });
        }
        CommandResult::from_event(JobEvent::JobPriorityForced(JobPriorityForced {
            job_id: self.id(),
            priority,
            forced_by,
            originating_job_id,
            requested_priority: originating_job_id.map(|_| priority),
        }))
    }

    /// The effective level a waiting, non-forced Job takes once the maximum
    /// of its user changed. A Job that is not waiting, is forced, or whose
    /// level does not move records nothing.
    pub fn recalculate_priority(
        &self,
        basis: PriorityBasis,
        user_id: KnownUserId,
    ) -> JobCommandResult {
        self.recalculated_on(basis, Some(user_id))
    }

    /// The effective level a Job that just went back to waiting takes from
    /// the current cap or its parent — a retry or a withdrawn trigger waits at
    /// the level its basis gives now, not the one it was admitted with. A
    /// forced Job keeps its force.
    pub fn rederive_priority(&self, basis: PriorityBasis) -> JobCommandResult {
        self.recalculated_on(basis, None)
    }

    fn recalculated_on(
        &self,
        basis: PriorityBasis,
        user_id: Option<KnownUserId>,
    ) -> JobCommandResult {
        if !self.is_waiting() {
            return CommandResult::new(vec![]);
        }
        match self.priority().recalculated(basis) {
            None => CommandResult::new(vec![]),
            Some(effective_priority) => CommandResult::from_event(
                JobEvent::JobPriorityRecalculated(JobPriorityRecalculated {
                    job_id: self.id(),
                    effective_priority,
                    previous_priority: self.priority().effective(),
                    user_id,
                    requested_priority: match basis {
                        PriorityBasis::Capped { .. } => None,
                        PriorityBasis::InheritedFromParent { .. }
                        | PriorityBasis::ForcedByParent { .. } => Some(effective_priority),
                    },
                }),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::access::PrincipalKind;
    use crate::domain::job::resolution::JobResolution;
    use crate::fixtures::{JobBuilder, RunBuilder, job_id, resolution_id, ts, user};
    use uuid::Uuid;

    fn level(value: i64) -> Priority {
        Priority::new(value).unwrap()
    }

    fn super_user() -> PrincipalFacts {
        PrincipalFacts::new(PrincipalKind::Human, true, true)
    }

    fn force(value: i64) -> ForcePriority {
        ForcePriority {
            priority: level(value),
            forced_by: user(),
        }
    }

    fn some_user() -> KnownUserId {
        KnownUserId::new(Uuid::now_v7()).unwrap()
    }

    #[test]
    fn a_super_user_forces_a_waiting_job_above_its_users_cap() {
        // Given: a waiting job capped at 2
        let job = JobBuilder::new()
            .with_priority(level(5), level(2), None)
            .build();
        let command = force(5);
        let forced_by = command.forced_by.clone();
        // When: a super user forces it to 5
        let result = job.force_priority(command, &super_user()).unwrap();
        // Then: one fact records the level and who forced it, as the job a user forced
        assert_eq!(
            result.events,
            vec![JobEvent::JobPriorityForced(JobPriorityForced {
                job_id: job.id(),
                priority: level(5),
                forced_by,
                originating_job_id: None,
                requested_priority: None,
            })]
        );
    }

    #[test]
    fn a_member_forcing_a_priority_is_refused_with_the_affordance_code() {
        // Given: a waiting job and an ordinary member
        let job = JobBuilder::new().build();
        let member = PrincipalFacts::new(PrincipalKind::Human, true, false);
        // When: the member forces it
        let result = job.force_priority(force(5), &member);
        // Then: the refusal is the one the affordance advertises
        assert_eq!(result, Err(JobsError::NotSuperUser));
        assert_eq!(
            job.can_force_priority(&member).reason_code(),
            Some("not_super_user")
        );
    }

    #[test]
    fn a_terminal_job_cannot_be_forced() {
        // Given: a completed job
        let job = JobBuilder::new()
            .with_resolution(JobResolution::completed(resolution_id(), ts(30)))
            .build();
        // When/Then: forcing is refused on the lifecycle
        assert_eq!(
            job.force_priority(force(5), &super_user()),
            Err(JobsError::JobAlreadyTerminal {
                status: "COMPLETED"
            })
        );
    }

    #[test]
    fn forcing_the_level_a_job_is_already_forced_to_records_nothing() {
        // Given: a job already forced to 4
        let job = JobBuilder::new()
            .with_priority(level(3), level(4), Some(level(4)))
            .build();
        // When: it is forced to 4 again
        let result = job.force_priority(force(4), &super_user()).unwrap();
        // Then: nothing is recorded twice
        assert!(result.is_empty());
    }

    #[test]
    fn a_forced_job_may_be_forced_again_to_another_level() {
        // Given: a job forced to 5
        let job = JobBuilder::new()
            .with_priority(level(3), level(5), Some(level(5)))
            .build();
        // When: it is forced to 1
        let result = job.force_priority(force(1), &super_user()).unwrap();
        // Then: the new level holds until forced again
        assert!(matches!(
            result.events.as_slice(),
            [JobEvent::JobPriorityForced(fact)] if fact.priority == level(1)
        ));
    }

    #[test]
    fn a_waiting_descendant_is_forced_naming_the_job_a_super_user_forced() {
        // Given: a waiting child of a forced parent
        let parent = job_id();
        let child = JobBuilder::new().with_parent(parent).build();
        // When: the cascade reaches it
        let result = child.force_priority_as_descendant(force(5), parent);
        // Then: it is forced to the same level, naming the origin, and records that level as
        // its request — an inheriting descendant requests what it inherits
        assert!(matches!(
            result.events.as_slice(),
            [JobEvent::JobPriorityForced(fact)]
                if fact.originating_job_id == Some(parent)
                    && fact.requested_priority == Some(level(5))
        ));
    }

    #[test]
    fn a_waiting_descendant_forced_earlier_takes_the_new_forced_level() {
        // Given: a waiting child forced earlier to 5 through its parent
        let parent = job_id();
        let child = JobBuilder::new()
            .with_parent(parent)
            .with_priority(level(5), level(5), Some(level(5)))
            .build();
        // When: the parent is forced again, to 4
        let result = child.force_priority_as_descendant(force(4), parent);
        // Then: the child follows the latest force
        assert!(matches!(
            result.events.as_slice(),
            [JobEvent::JobPriorityForced(fact)]
                if fact.priority == level(4) && fact.requested_priority == Some(level(4))
        ));
    }

    #[test]
    fn a_descendant_already_forced_to_that_level_records_nothing() {
        // Given: a waiting child forced to 4
        let parent = job_id();
        let child = JobBuilder::new()
            .with_parent(parent)
            .with_priority(level(4), level(4), Some(level(4)))
            .build();
        // When/Then: a cascade to 4 records nothing
        assert!(
            child
                .force_priority_as_descendant(force(4), parent)
                .is_empty()
        );
    }

    #[test]
    fn a_descendant_that_stopped_waiting_is_left_alone() {
        // Given: a child already running
        let parent = job_id();
        let child = JobBuilder::new()
            .with_parent(parent)
            .with_run(RunBuilder::new(1).started(ts(5)).build())
            .build();
        // When: the cascade reaches it
        // Then: nothing — a forced level only matters while a job waits
        assert!(
            child
                .force_priority_as_descendant(force(5), parent)
                .is_empty()
        );
    }

    #[test]
    fn a_new_cap_recalculates_a_waiting_unforced_job() {
        // Given: a waiting job that requested 5 under a cap of 3
        let job = JobBuilder::new()
            .with_priority(level(5), level(3), None)
            .build();
        let user_id = some_user();
        // When: the cap of its user is raised to 4
        let result = job.recalculate_priority(PriorityBasis::Capped { maximum: level(4) }, user_id);
        // Then: it now waits at 4, the change naming its previous level and the user
        assert_eq!(
            result.events,
            vec![JobEvent::JobPriorityRecalculated(JobPriorityRecalculated {
                job_id: job.id(),
                effective_priority: level(4),
                previous_priority: level(3),
                user_id: Some(user_id),
                requested_priority: None,
            })]
        );
    }

    #[test]
    fn a_job_back_to_waiting_takes_the_level_its_cap_gives_now() {
        // Given: a job admitted at 4 that requested 5, whose first attempt failed
        // with a retry scheduled
        let job = JobBuilder::new()
            .with_priority(level(5), level(4), None)
            .with_run(
                RunBuilder::new(1)
                    .started(ts(5))
                    .failed(
                        ts(20),
                        crate::domain::run::failure::RunFailureKind::Transient,
                    )
                    .retry_due(ts(80))
                    .build(),
            )
            .build();
        // When: it re-derives its level under a cap now at 2
        let result = job.rederive_priority(PriorityBasis::Capped { maximum: level(2) });
        // Then: it waits at 2, naming no user
        assert!(matches!(
            result.events.as_slice(),
            [JobEvent::JobPriorityRecalculated(fact)]
                if fact.effective_priority == level(2) && fact.user_id.is_none()
        ));
    }

    #[test]
    fn a_forced_job_back_to_waiting_keeps_its_force() {
        // Given: a forced job waiting for its retry
        let job = JobBuilder::new()
            .with_priority(level(3), level(5), Some(level(5)))
            .with_run(
                RunBuilder::new(1)
                    .started(ts(5))
                    .failed(
                        ts(20),
                        crate::domain::run::failure::RunFailureKind::Transient,
                    )
                    .retry_due(ts(80))
                    .build(),
            )
            .build();
        // When/Then: the cap does not move it
        assert!(
            job.rederive_priority(PriorityBasis::Capped { maximum: level(1) })
                .is_empty()
        );
    }

    #[test]
    fn a_forced_job_keeps_its_forced_level_when_its_users_cap_changes() {
        // Given: a waiting job forced to 5
        let job = JobBuilder::new()
            .with_priority(level(3), level(5), Some(level(5)))
            .build();
        // When: its user's cap is lowered to 1
        // Then: nothing changes
        assert!(
            job.recalculate_priority(PriorityBasis::Capped { maximum: level(1) }, some_user())
                .is_empty()
        );
    }

    #[test]
    fn a_running_job_is_not_recalculated() {
        // Given: a job executing at 3 that requested 5
        let job = JobBuilder::new()
            .with_priority(level(5), level(3), None)
            .with_run(RunBuilder::new(1).started(ts(5)).build())
            .build();
        // When: its user's cap is raised
        // Then: priority orders waiting jobs only — nothing is recorded
        assert!(
            job.recalculate_priority(PriorityBasis::Capped { maximum: level(5) }, some_user())
                .is_empty()
        );
    }
}
