use crate::domain::ids::{AgentId, RunId};
use crate::domain::job::JobCall;
use crate::domain::payload::CallMode;
use crate::domain::requester::Requester;
use crate::event::job::JobEvent;
use crate::event::run_inbox::RunInboxEffect;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoticePlan {
    Accepted,
    QueuePosition,
    Started,
    RunCompleted { run_id: RunId },
    JobFailed,
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
        let background = self.mode == CallMode::Background;
        match event.run_inbox_effect() {
            RunInboxEffect::JobQueued if background => {
                vec![NoticePlan::Accepted, NoticePlan::QueuePosition]
            }
            RunInboxEffect::RetryScheduled if background => vec![NoticePlan::QueuePosition],
            RunInboxEffect::RunCancelled { withdrawn: true } if background => {
                vec![NoticePlan::QueuePosition]
            }
            RunInboxEffect::RunStarted if background => vec![NoticePlan::Started],
            RunInboxEffect::RunCompleted { run_id } => vec![NoticePlan::RunCompleted { run_id }],
            RunInboxEffect::JobFailed => vec![NoticePlan::JobFailed],
            RunInboxEffect::Untouched
            | RunInboxEffect::JobQueued
            | RunInboxEffect::RetryScheduled
            | RunInboxEffect::RunStarted
            | RunInboxEffect::RunFailed
            | RunInboxEffect::RunCancelled { .. } => vec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::job::resolution::JobFailureCause;
    use crate::domain::keys::ReasonCode;
    use crate::event::job_facts::{JobFailed, RunCancelled, RunCompleted, RunStarted};
    use crate::event::run_inbox::samples::every_kind_of_event;
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
    fn no_event_tells_a_caller_unless_its_write_serializes_with_the_directory() {
        for (event, effect) in every_kind_of_event() {
            for notices in [background(), sync()] {
                assert!(
                    notices.plan(&event).is_empty() || effect.serializes_with_the_tree_directory(),
                    "{} tells a caller, so its write serializes with the inbox",
                    event.event_type()
                );
            }
        }
    }

    #[test]
    fn every_kind_of_event_tells_a_background_caller_exactly_what_the_intent_lists() {
        let told: Vec<(&str, Vec<NoticePlan>)> = every_kind_of_event()
            .into_iter()
            .map(|(event, _)| (event.event_type(), background().plan(&event)))
            .filter(|(_, plan)| !plan.is_empty())
            .collect();
        assert_eq!(
            told.iter().map(|(kind, _)| *kind).collect::<Vec<_>>(),
            [
                "JobQueued",
                "RunStarted",
                "RunCompleted",
                "RunCancelled",
                "RetryScheduled",
                "JobFailed"
            ]
        );
        assert_eq!(
            told[0].1,
            vec![NoticePlan::Accepted, NoticePlan::QueuePosition]
        );
        assert_eq!(told[4].1, vec![NoticePlan::QueuePosition]);
    }

    #[test]
    fn a_run_withdrawn_for_a_waiting_job_tells_the_queue_position_again() {
        let withdrawn = JobEvent::RunCancelled(RunCancelled {
            job_id: job_id(),
            run_id: run_id(),
            withdrawn_for: Some(ReasonCode::new("job_cancelled").unwrap()),
        });
        let cancelled = JobEvent::RunCancelled(RunCancelled {
            job_id: job_id(),
            run_id: run_id(),
            withdrawn_for: None,
        });
        assert_eq!(
            background().plan(&withdrawn),
            vec![NoticePlan::QueuePosition]
        );
        assert!(background().plan(&cancelled).is_empty());
        assert!(sync().plan(&withdrawn).is_empty());
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
