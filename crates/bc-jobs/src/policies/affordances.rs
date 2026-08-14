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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::job::resolution::JobResolution;
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
}
