use std::collections::HashMap;

use crate::commands::JobCommandResult;
use crate::domain::ids::JobId;
use crate::domain::job::Job;
use crate::domain::policy::ServiceLimits;
use crate::domain::priority::{Priority, PriorityBasis, UserMaxPriority};
use crate::event::job::JobEvent;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ForceDescendant {
    pub job_id: JobId,
    pub originating_job_id: JobId,
}

/// Forcing a Job also forces its waiting descendants that are not forced
/// already; descendants running, settled or forced to their own level keep it.
pub fn forcing_cascade(forced: &Job, descendants: &[Job]) -> Vec<ForceDescendant> {
    descendants
        .iter()
        .filter(|descendant| descendant.is_waiting() && !descendant.priority().is_forced())
        .map(|descendant| ForceDescendant {
            job_id: descendant.id(),
            originating_job_id: forced.id(),
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Recalculation {
    pub job_id: JobId,
    pub result: JobCommandResult,
}

/// A user's maximum changed: their waiting, non-forced root Jobs are capped
/// again, and the waiting, non-forced descendants that inherit from a root
/// whose level moved take its new level, generation by generation. A child of
/// the user is never capped on its own — it inherits. `jobs` are the user's
/// Jobs; `descendants` the descendants of those Jobs.
pub fn recalculate_for_user(
    user: &UserMaxPriority,
    jobs: &[Job],
    descendants: &[Job],
    limits: &ServiceLimits,
) -> Vec<Recalculation> {
    let maximum = user.applicable(limits);
    let mut moved: HashMap<JobId, Priority> = HashMap::new();
    let mut recalculated = Vec::new();
    let mut roots: Vec<&Job> = jobs
        .iter()
        .filter(|job| job.parent_job_id().is_none() && runs_for(job, user))
        .collect();
    roots.sort_by_key(|job| (job.created_at(), job.id()));
    for root in roots {
        let result = root.recalculate_priority(PriorityBasis::Capped { maximum }, user.user_id());
        record(&mut moved, &mut recalculated, root.id(), result);
    }
    let mut inheriting: Vec<&Job> = descendants.iter().collect();
    inheriting.sort_by_key(|job| (job.created_at(), job.id()));
    for descendant in inheriting {
        let Some(parent_effective) = descendant
            .parent_job_id()
            .and_then(|parent| moved.get(&parent).copied())
        else {
            continue;
        };
        let result = descendant.recalculate_priority(
            PriorityBasis::InheritedFromParent { parent_effective },
            user.user_id(),
        );
        record(&mut moved, &mut recalculated, descendant.id(), result);
    }
    recalculated
}

fn record(
    moved: &mut HashMap<JobId, Priority>,
    recalculated: &mut Vec<Recalculation>,
    job_id: JobId,
    result: JobCommandResult,
) {
    for event in &result.events {
        if let JobEvent::JobPriorityRecalculated(fact) = event {
            moved.insert(fact.job_id, fact.effective_priority);
        }
    }
    if !result.is_empty() {
        recalculated.push(Recalculation { job_id, result });
    }
}

fn runs_for(job: &Job, user: &UserMaxPriority) -> bool {
    job.triggered_by()
        .is_some_and(|known| known.id().0 == user.user_id().as_uuid())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::ids::KnownUserId;
    use crate::domain::job::resolution::JobResolution;
    use crate::domain::keys::DisplayName;
    use crate::domain::references::KnownUser;
    use crate::fixtures::{JobBuilder, RunBuilder, resolution_id, ts};
    use br_core_events::UserId;
    use uuid::Uuid;

    fn level(value: i64) -> Priority {
        Priority::new(value).unwrap()
    }

    fn known() -> KnownUser {
        KnownUser::new(UserId(Uuid::now_v7()), DisplayName::new("Ada").unwrap()).unwrap()
    }

    fn capped(user: &KnownUser, maximum: i64) -> UserMaxPriority {
        UserMaxPriority::new(KnownUserId::new(user.id().0).unwrap(), Some(level(maximum)))
    }

    #[test]
    fn forcing_skips_a_descendant_already_forced_to_its_own_level() {
        // Given: a forced parent with a waiting child forced earlier to 1
        let parent = JobBuilder::new().build();
        let forced = JobBuilder::new()
            .with_parent(parent.id())
            .with_priority(level(3), level(1), Some(level(1)))
            .build();
        // When/Then: the cascade leaves it alone
        assert!(forcing_cascade(&parent, &[forced]).is_empty());
    }

    #[test]
    fn forcing_reaches_waiting_descendants_only() {
        // Given: a forced parent with a waiting child, a running child and a settled one
        let parent = JobBuilder::new().build();
        let waiting = JobBuilder::new().with_parent(parent.id()).build();
        let running = JobBuilder::new()
            .with_parent(parent.id())
            .with_run(RunBuilder::new(1).started(ts(5)).build())
            .build();
        let settled = JobBuilder::new()
            .with_parent(parent.id())
            .with_resolution(JobResolution::completed(resolution_id(), ts(30)))
            .build();
        // When: the cascade is computed
        let cascade = forcing_cascade(&parent, &[waiting.clone(), running, settled]);
        // Then: only the waiting child is forced, naming the parent
        assert_eq!(
            cascade,
            vec![ForceDescendant {
                job_id: waiting.id(),
                originating_job_id: parent.id(),
            }]
        );
    }

    #[test]
    fn raising_a_users_cap_recalculates_their_waiting_unforced_jobs_only() {
        // Given: a user capped at 3, with a capped waiting job, a forced one and a running one
        let user = known();
        let waiting = JobBuilder::new()
            .with_triggered_by(user.clone())
            .with_priority(level(5), level(3), None)
            .build();
        let forced = JobBuilder::new()
            .with_triggered_by(user.clone())
            .with_priority(level(5), level(5), Some(level(5)))
            .build();
        let running = JobBuilder::new()
            .with_triggered_by(user.clone())
            .with_priority(level(5), level(3), None)
            .with_run(RunBuilder::new(1).started(ts(5)).build())
            .build();
        // When: a super user raises the cap to 4
        let recalculated = recalculate_for_user(
            &capped(&user, 4),
            &[waiting.clone(), forced, running],
            &[],
            &ServiceLimits::default(),
        );
        // Then: only the waiting, non-forced job moves, to 4
        assert_eq!(recalculated.len(), 1);
        assert_eq!(recalculated[0].job_id, waiting.id());
        assert!(matches!(
            recalculated[0].result.events.as_slice(),
            [JobEvent::JobPriorityRecalculated(fact)] if fact.effective_priority == level(4)
        ));
    }

    #[test]
    fn a_waiting_child_follows_its_root_recalculated_in_the_same_change() {
        // Given: a user's root waiting at 3, its waiting child and grandchild at 3, and a
        // forced grandchild
        let user = known();
        let root = JobBuilder::new()
            .with_triggered_by(user.clone())
            .with_priority(level(5), level(3), None)
            .build();
        let child = JobBuilder::new()
            .with_parent(root.id())
            .with_priority(level(3), level(3), None)
            .build();
        let grandchild = JobBuilder::new()
            .with_parent(child.id())
            .with_priority(level(3), level(3), None)
            .build();
        let forced = JobBuilder::new()
            .with_parent(child.id())
            .with_priority(level(3), level(1), Some(level(1)))
            .build();
        // When: the user's cap is raised to 5
        let recalculated = recalculate_for_user(
            &capped(&user, 5),
            std::slice::from_ref(&root),
            &[grandchild.clone(), forced, child.clone()],
            &ServiceLimits::default(),
        );
        // Then: the root moves to 5, the inheriting descendants follow and record 5 as their
        // request, the forced one stays
        let moved: Vec<(JobId, Priority)> = recalculated
            .iter()
            .flat_map(|change| change.result.events.iter())
            .filter_map(|event| match event {
                JobEvent::JobPriorityRecalculated(fact) => {
                    Some((fact.job_id, fact.effective_priority))
                }
                _ => None,
            })
            .collect();
        assert_eq!(
            moved,
            vec![
                (root.id(), level(5)),
                (child.id(), level(5)),
                (grandchild.id(), level(5))
            ]
        );
        let requested: Vec<Option<Priority>> = recalculated
            .iter()
            .flat_map(|change| change.result.events.iter())
            .filter_map(|event| match event {
                JobEvent::JobPriorityRecalculated(fact) => Some(fact.requested_priority),
                _ => None,
            })
            .collect();
        assert_eq!(requested, vec![None, Some(level(5)), Some(level(5))]);
    }

    #[test]
    fn another_users_job_is_never_recalculated() {
        // Given: a waiting job of somebody else
        let other = JobBuilder::new()
            .with_triggered_by(known())
            .with_priority(level(5), level(3), None)
            .build();
        // When: this user's cap changes
        let recalculated = recalculate_for_user(
            &capped(&known(), 5),
            &[other],
            &[],
            &ServiceLimits::default(),
        );
        // Then: nothing of theirs moves
        assert!(recalculated.is_empty());
    }
}
