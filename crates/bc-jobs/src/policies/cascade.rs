use crate::domain::ids::JobId;
use crate::domain::job::Job;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CancelDescendant {
    pub job_id: JobId,
    pub originating_job_id: JobId,
}

pub fn cancellation_cascade(cancelled: &Job, descendants: &[Job]) -> Vec<CancelDescendant> {
    descendants
        .iter()
        .filter(|descendant| !descendant.is_terminal())
        .map(|descendant| CancelDescendant {
            job_id: descendant.id(),
            originating_job_id: cancelled.id(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::job::resolution::JobResolution;
    use crate::fixtures::{JobBuilder, RunBuilder, resolution_id, ts};

    #[test]
    fn cancellation_travels_down_to_every_live_descendant() {
        // Given: a cancelled job with two live children and one already settled
        let parent = JobBuilder::new().build();
        let live_one = JobBuilder::new().with_parent(parent.id()).build();
        let live_two = JobBuilder::new()
            .with_parent(parent.id())
            .with_run(RunBuilder::new(1).started(ts(5)).build())
            .build();
        let settled = JobBuilder::new()
            .with_parent(parent.id())
            .with_resolution(JobResolution::completed(resolution_id(), ts(30)))
            .build();
        // When: the cascade is computed
        let intents = cancellation_cascade(&parent, &[live_one.clone(), live_two.clone(), settled]);
        // Then: only the live descendants are cancelled, each naming the origin
        assert_eq!(
            intents,
            vec![
                CancelDescendant {
                    job_id: live_one.id(),
                    originating_job_id: parent.id()
                },
                CancelDescendant {
                    job_id: live_two.id(),
                    originating_job_id: parent.id()
                },
            ]
        );
    }

    #[test]
    fn a_job_with_no_descendant_cascades_nothing() {
        // Given: a leaf job
        let job = JobBuilder::new().build();
        // When/Then: the cascade is empty
        assert!(cancellation_cascade(&job, &[]).is_empty());
    }
}
