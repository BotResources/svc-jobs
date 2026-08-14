use crate::domain::ids::JobId;
use crate::domain::job::Job;
use crate::error::JobsError;

#[derive(Debug, Clone, Copy)]
pub enum ParentContext<'a> {
    Root,
    Loaded(&'a Job),
}

impl<'a> ParentContext<'a> {
    pub fn of(parent: Option<&'a Job>) -> Self {
        match parent {
            Some(parent) => Self::Loaded(parent),
            None => Self::Root,
        }
    }

    pub fn resolve(self, parent_job_id: Option<JobId>) -> Result<Option<&'a Job>, JobsError> {
        match (self, parent_job_id) {
            (Self::Root, None) => Ok(None),
            (Self::Loaded(parent), Some(expected)) if parent.id() == expected => Ok(Some(parent)),
            (Self::Loaded(_), None) => Err(JobsError::CorruptState {
                reason_code: "parent_supplied_for_a_root_job",
            }),
            _ => Err(JobsError::CorruptState {
                reason_code: "parent_job_not_loaded",
            }),
        }
    }
}

pub fn guard_parent_admits_work(
    parent_job_id: JobId,
    parent: Option<&Job>,
) -> Result<&Job, JobsError> {
    let parent = parent
        .filter(|candidate| candidate.id() == parent_job_id)
        .ok_or(JobsError::ParentJobUnknown {
            parent_job_id: parent_job_id.as_uuid(),
        })?;
    guard_admits_work(parent)
}

pub fn guard_admits_work(parent: &Job) -> Result<&Job, JobsError> {
    if parent.is_deleted() {
        return Err(JobsError::ParentJobDeleted {
            parent_job_id: parent.id().as_uuid(),
        });
    }
    if parent.is_terminal() {
        return Err(JobsError::ParentJobTerminal {
            parent_job_id: parent.id().as_uuid(),
        });
    }
    Ok(parent)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::job::resolution::JobResolution;
    use crate::fixtures::{JobBuilder, RunBuilder, job_id, resolution_id, ts};

    fn live_parent() -> Job {
        JobBuilder::new()
            .with_run(RunBuilder::new(1).started(ts(5)).build())
            .build()
    }

    #[test]
    fn a_child_judged_without_its_parent_loaded_is_refused_rather_than_assumed_orphaned() {
        // Given: a child job whose parent was not loaded alongside it
        let child = JobBuilder::new().with_parent(job_id()).build();
        // When: the parent context is resolved
        let result = ParentContext::Root.resolve(child.parent_job_id());
        // Then: the wiring gap fails loud instead of reading as an unknown parent
        assert_eq!(
            result.err(),
            Some(JobsError::CorruptState {
                reason_code: "parent_job_not_loaded"
            })
        );
    }

    #[test]
    fn a_child_judged_against_someone_elses_parent_is_refused() {
        // Given: a child job and a job that is not its parent
        let child = JobBuilder::new().with_parent(job_id()).build();
        let stranger = live_parent();
        // When: the stranger is offered as the parent
        let result = ParentContext::Loaded(&stranger).resolve(child.parent_job_id());
        // Then: identity is pinned — the wrong parent never stands in for the right one
        assert_eq!(
            result.err(),
            Some(JobsError::CorruptState {
                reason_code: "parent_job_not_loaded"
            })
        );
    }

    #[test]
    fn a_parent_offered_for_a_root_job_is_refused() {
        // Given: a job that has no parent at all
        let root = JobBuilder::new().build();
        let stranger = live_parent();
        // When: a parent is supplied anyway
        let result = ParentContext::Loaded(&stranger).resolve(root.parent_job_id());
        // Then: the contradiction is refused rather than quietly ignored
        assert_eq!(
            result.err(),
            Some(JobsError::CorruptState {
                reason_code: "parent_supplied_for_a_root_job"
            })
        );
    }

    #[test]
    fn the_parent_a_child_names_resolves_to_that_parent() {
        // Given: a child loaded together with its own parent
        let parent = live_parent();
        let child = JobBuilder::new().with_parent(parent.id()).build();
        // When: the context is resolved
        let resolved = ParentContext::of(Some(&parent))
            .resolve(child.parent_job_id())
            .unwrap();
        // Then: the parent is handed to the rule that needs it
        assert_eq!(resolved.map(Job::id), Some(parent.id()));
    }

    #[test]
    fn a_root_job_resolves_to_no_parent_at_all() {
        // Given: a job declared by a producer with no parent
        let root = JobBuilder::new().build();
        // When: the context is resolved
        let resolved = ParentContext::Root.resolve(root.parent_job_id()).unwrap();
        // Then: nothing is expected of a parent
        assert!(resolved.is_none());
    }

    #[test]
    fn a_parent_that_has_finished_admits_no_further_work() {
        // Given: a parent job that already reached a terminal resolution
        let parent = JobBuilder::new()
            .with_resolution(JobResolution::completed(resolution_id(), ts(30)))
            .build();
        // When: work is offered under it
        let result = guard_admits_work(&parent);
        // Then: it is refused, naming the parent
        assert_eq!(
            result.err(),
            Some(JobsError::ParentJobTerminal {
                parent_job_id: parent.id().as_uuid()
            })
        );
    }
}
