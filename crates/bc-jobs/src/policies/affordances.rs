use crate::domain::job::Job;
use crate::event::job::JobEvent;
use crate::event::job_facts::JobAffordancesChanged;

pub fn predecessor_affordances_changed(settled_successor: &Job) -> Option<JobEvent> {
    let predecessor_job_id = settled_successor.predecessor_job_id()?;
    settled_successor.is_terminal().then(|| {
        JobEvent::JobAffordancesChanged(JobAffordancesChanged {
            job_id: predecessor_job_id,
            caused_by_job_id: Some(settled_successor.id()),
        })
    })
}

pub fn descendant_affordances_changed(settled_parent: &Job, children: &[Job]) -> Vec<JobEvent> {
    if !settled_parent.is_terminal() {
        return vec![];
    }
    children
        .iter()
        .filter(|child| child.is_terminal())
        .map(|child| {
            JobEvent::JobAffordancesChanged(JobAffordancesChanged {
                job_id: child.id(),
                caused_by_job_id: Some(settled_parent.id()),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::job::resolution::{JobFailureCause, JobResolution};
    use crate::fixtures::{JobBuilder, RunBuilder, job_id, resolution_id, ts};

    #[test]
    fn a_settling_successor_flips_its_predecessors_affordances() {
        // Given: a manual-retry successor that has just reached a terminal state
        let predecessor = job_id();
        let successor = JobBuilder::new()
            .with_predecessor(predecessor)
            .with_resolution(JobResolution::completed(resolution_id(), ts(30)))
            .build();
        // When: the policy reacts
        let pushed = predecessor_affordances_changed(&successor);
        // Then: the predecessor is told its delete action just opened, with no state change
        match pushed {
            Some(JobEvent::JobAffordancesChanged(fact)) => {
                assert_eq!(fact.job_id, predecessor);
                assert_eq!(fact.caused_by_job_id, Some(successor.id()));
            }
            other => panic!("expected an affordance change, got {other:?}"),
        }
    }

    #[test]
    fn a_live_successor_flips_nothing_yet() {
        // Given: a successor still running
        let successor = JobBuilder::new()
            .with_predecessor(job_id())
            .with_run(RunBuilder::new(1).started(ts(5)).build())
            .build();
        // When/Then: the predecessor's affordances are unchanged
        assert_eq!(predecessor_affordances_changed(&successor), None);
    }

    #[test]
    fn a_job_with_no_predecessor_flips_nothing() {
        // Given: a job that is nobody's successor
        let job = JobBuilder::new()
            .with_resolution(JobResolution::completed(resolution_id(), ts(30)))
            .build();
        // When/Then: no affordance push is owed to anyone
        assert_eq!(predecessor_affordances_changed(&job), None);
    }

    fn failed(parent: Option<&Job>) -> Job {
        let builder = JobBuilder::new().with_resolution(
            JobResolution::failed(
                resolution_id(),
                ts(30),
                JobFailureCause::DeclaredByOwner,
                None,
            )
            .unwrap(),
        );
        match parent {
            Some(parent) => builder.with_parent(parent.id()).build(),
            None => builder.build(),
        }
    }

    #[test]
    fn a_settling_parent_flips_the_affordances_of_its_already_terminal_children() {
        // Given: a parent that resolves after a child of its own had already failed
        let parent = failed(None);
        let child = failed(Some(&parent));
        // When: the parent settles
        let pushed = descendant_affordances_changed(&parent, std::slice::from_ref(&child));
        // Then: the child is told its retry closed, without any state change of its own
        match pushed.as_slice() {
            [JobEvent::JobAffordancesChanged(fact)] => {
                assert_eq!(fact.job_id, child.id());
                assert_eq!(fact.caused_by_job_id, Some(parent.id()));
            }
            other => panic!("expected one affordance change, got {other:?}"),
        }
    }

    #[test]
    fn a_live_child_is_left_to_the_cancellation_cascade() {
        // Given: a settled parent with one live child, which the cascade will cancel
        let parent = failed(None);
        let live = JobBuilder::new()
            .with_parent(parent.id())
            .with_run(RunBuilder::new(1).started(ts(5)).build())
            .build();
        // When: the parent settles
        // Then: no affordance push — the child's own state change carries the news
        assert!(descendant_affordances_changed(&parent, &[live]).is_empty());
    }

    #[test]
    fn a_parent_still_running_flips_nothing_for_its_children() {
        // Given: a parent still executing above an already-failed child
        let parent = JobBuilder::new()
            .with_run(RunBuilder::new(1).started(ts(5)).build())
            .build();
        let child = failed(Some(&parent));
        // When/Then: the retry stays open, so nothing is pushed
        assert!(descendant_affordances_changed(&parent, &[child]).is_empty());
    }
}
