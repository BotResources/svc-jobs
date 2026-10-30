use crate::domain::ids::JobId;
use crate::event::job::JobEvent;
use crate::event::job_facts::JobQueued;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueuedTree {
    Named(JobId),
    BelowParent(JobId),
    Itself(JobId),
}

impl JobQueued {
    pub fn tree(&self) -> QueuedTree {
        match (self.initial_job_id, self.parent_job_id) {
            (Some(tree), _) => QueuedTree::Named(tree),
            (None, Some(parent)) => QueuedTree::BelowParent(parent),
            (None, None) => QueuedTree::Itself(self.job_id),
        }
    }
}

impl JobEvent {
    pub fn queued(&self) -> Option<&JobQueued> {
        match self {
            Self::JobQueued(fact) => Some(fact),
            Self::RunDispatched(_)
            | Self::RunStarted(_)
            | Self::RunPlanDeclared(_)
            | Self::RunStepStarted(_)
            | Self::RunCompleted(_)
            | Self::RunFailed(_)
            | Self::RunCancellationRequested(_)
            | Self::RunCancelled(_)
            | Self::RetryScheduled(_)
            | Self::JobCompleted(_)
            | Self::JobFailed(_)
            | Self::JobCancelled(_)
            | Self::ManualRetryStarted(_)
            | Self::JobDeleted(_)
            | Self::JobAffordancesChanged(_)
            | Self::JobPriorityForced(_)
            | Self::JobPriorityRecalculated(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::run_inbox::samples::{Kind, every_kind_of_event, kind_of};
    use crate::fixtures::job_id;

    fn queued_with(initial: Option<JobId>, parent: Option<JobId>) -> JobEvent {
        let (JobEvent::JobQueued(mut fact), _) = every_kind_of_event()
            .into_iter()
            .find(|(event, _)| kind_of(event) == Kind::JobQueued)
            .expect("a queued sample")
        else {
            panic!("the queued sample is a queued event");
        };
        fact.initial_job_id = initial;
        fact.parent_job_id = parent;
        JobEvent::JobQueued(fact)
    }

    fn tree_of(event: &JobEvent) -> QueuedTree {
        event.queued().expect("a queued event").tree()
    }

    #[test]
    fn a_job_that_names_its_tree_belongs_to_it_whatever_its_parent() {
        let tree = job_id();
        let parent = job_id();
        assert_eq!(
            tree_of(&queued_with(Some(tree), Some(parent))),
            QueuedTree::Named(tree)
        );
        assert_eq!(
            tree_of(&queued_with(Some(tree), None)),
            QueuedTree::Named(tree)
        );
    }

    #[test]
    fn a_job_with_a_parent_and_no_named_tree_belongs_to_the_tree_of_its_parent() {
        let parent = job_id();
        assert_eq!(
            tree_of(&queued_with(None, Some(parent))),
            QueuedTree::BelowParent(parent)
        );
    }

    #[test]
    fn a_job_with_neither_is_the_root_of_its_own_tree() {
        let event = queued_with(None, None);
        assert_eq!(tree_of(&event), QueuedTree::Itself(event.job_id()));
    }

    #[test]
    fn only_the_queued_event_queues_a_job() {
        for (event, _) in every_kind_of_event() {
            assert_eq!(
                event.queued().is_some(),
                kind_of(&event) == Kind::JobQueued,
                "{}",
                event.event_type()
            );
        }
    }
}
