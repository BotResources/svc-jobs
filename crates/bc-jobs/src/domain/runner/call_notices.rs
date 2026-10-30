use crate::domain::ids::{AgentId, RunId};
use crate::domain::job::JobCall;
use crate::domain::payload::CallMode;
use crate::domain::requester::Requester;
use crate::event::job::JobEvent;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoticePlan {
    Accepted,
    QueuePosition,
    Started,
    RunCompleted { run_id: RunId },
    JobFailed,
}

pub fn may_notify_a_caller(event: &JobEvent) -> bool {
    matches!(
        event,
        JobEvent::JobQueued(_)
            | JobEvent::RetryScheduled(_)
            | JobEvent::RunCancelled(_)
            | JobEvent::RunStarted(_)
            | JobEvent::RunCompleted(_)
            | JobEvent::JobFailed(_)
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CallNotices {
    pub caller_run: RunId,
    pub mode: CallMode,
    pub address: Option<AgentId>,
}

impl CallNotices {
    pub fn of(requester: &Requester, call: Option<&JobCall>) -> Option<Self> {
        match (requester, call) {
            (Requester::Run(caller_run), Some(call)) => Some(Self {
                caller_run: *caller_run,
                mode: call.mode,
                address: call.caller_address.map(AgentId::new),
            }),
            _ => None,
        }
    }

    pub fn plan(&self, event: &JobEvent) -> Vec<NoticePlan> {
        if !may_notify_a_caller(event) {
            return vec![];
        }
        let background = self.mode == CallMode::Background;
        match event {
            JobEvent::JobQueued(_) if background => {
                vec![NoticePlan::Accepted, NoticePlan::QueuePosition]
            }
            JobEvent::RetryScheduled(_) if background => vec![NoticePlan::QueuePosition],
            JobEvent::RunCancelled(fact) if background && fact.withdrawn_for.is_some() => {
                vec![NoticePlan::QueuePosition]
            }
            JobEvent::RunStarted(_) if background => vec![NoticePlan::Started],
            JobEvent::RunCompleted(fact) => vec![NoticePlan::RunCompleted {
                run_id: fact.run_id,
            }],
            JobEvent::JobFailed(_) => vec![NoticePlan::JobFailed],
            _ => vec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::job::resolution::JobFailureCause;
    use crate::event::job_facts::{JobFailed, RunCompleted, RunStarted};
    use crate::fixtures::{instance, job_id, resolution_id, run_id};
    use uuid::Uuid;

    fn background() -> CallNotices {
        CallNotices {
            caller_run: run_id(),
            mode: CallMode::Background,
            address: Some(AgentId::new(Uuid::now_v7())),
        }
    }

    fn sync() -> CallNotices {
        CallNotices {
            caller_run: run_id(),
            mode: CallMode::Sync,
            address: None,
        }
    }

    fn started() -> JobEvent {
        JobEvent::RunStarted(RunStarted {
            job_id: job_id(),
            run_id: run_id(),
            runner_type: instance().runner_type().clone(),
            instance_key: instance().instance_key().clone(),
        })
    }

    fn completed() -> JobEvent {
        JobEvent::RunCompleted(RunCompleted {
            job_id: job_id(),
            run_id: run_id(),
            attempt_number: crate::domain::attempts::AttemptNumber::FIRST,
        })
    }

    fn failed() -> JobEvent {
        JobEvent::JobFailed(JobFailed {
            job_id: job_id(),
            resolution_id: resolution_id(),
            failure_cause: JobFailureCause::DeclaredByOwner,
            caused_by_run_id: None,
            report: None,
        })
    }

    #[test]
    fn every_event_that_tells_a_caller_is_one_that_may_notify_a_caller() {
        for event in [started(), completed(), failed()] {
            for notices in [background(), sync()] {
                if !notices.plan(&event).is_empty() {
                    assert!(
                        may_notify_a_caller(&event),
                        "{} tells a caller, so its write serializes with the inbox",
                        event.event_type()
                    );
                }
            }
        }
    }

    #[test]
    fn a_background_call_tells_its_caller_every_step_of_the_call() {
        let call = background();
        assert_eq!(call.plan(&started()), vec![NoticePlan::Started]);
        assert!(matches!(
            call.plan(&completed()).as_slice(),
            [NoticePlan::RunCompleted { .. }]
        ));
        assert_eq!(call.plan(&failed()), vec![NoticePlan::JobFailed]);
    }

    #[test]
    fn a_sync_call_tells_its_caller_only_the_end_and_the_failure() {
        let call = sync();
        assert!(call.plan(&started()).is_empty());
        assert!(matches!(
            call.plan(&completed()).as_slice(),
            [NoticePlan::RunCompleted { .. }]
        ));
        assert_eq!(call.plan(&failed()), vec![NoticePlan::JobFailed]);
    }

    #[test]
    fn only_a_job_with_a_run_requester_and_a_call_tells_a_caller() {
        let call = JobCall {
            mode: CallMode::Sync,
            caller_address: None,
        };
        assert!(CallNotices::of(&Requester::Service(None), Some(&call)).is_none());
        assert!(CallNotices::of(&Requester::Run(run_id()), None).is_none());
        assert!(CallNotices::of(&Requester::Run(run_id()), Some(&call)).is_some());
    }
}
