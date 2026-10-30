use crate::domain::directory::registration::RunStanding;
use uuid::Uuid;

use crate::domain::ids::JobId;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunRefusal {
    RunUnknown,
    RunNotStarted,
    RunEnded,
    RunRemoved,
    InstanceMismatch,
    Malformed,
}

impl RunRefusal {
    pub fn code(&self) -> &'static str {
        match self {
            Self::RunUnknown => "run_unknown",
            Self::RunNotStarted => "run_not_started",
            Self::RunEnded => "run_ended",
            Self::RunRemoved => "run_removed",
            Self::InstanceMismatch => "instance_mismatch",
            Self::Malformed => "malformed",
        }
    }

    pub fn message(&self) -> &'static str {
        match self {
            Self::RunUnknown => "Jobs holds no such run.",
            Self::RunNotStarted => "The run has not started.",
            Self::RunEnded => "The run has ended.",
            Self::RunRemoved => "The run was removed from the agent directory.",
            Self::InstanceMismatch => "The run is not held by the instance that made the call.",
            Self::Malformed => "The call names an id Jobs cannot read.",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkRefusal {
    RunUnknown,
    RunNotStarted,
    RunEnded,
    RunRemoved,
    InstanceMismatch,
    JobNotOfRun,
    Malformed,
}

impl From<RunRefusal> for WorkRefusal {
    fn from(refusal: RunRefusal) -> Self {
        match refusal {
            RunRefusal::RunUnknown => Self::RunUnknown,
            RunRefusal::RunNotStarted => Self::RunNotStarted,
            RunRefusal::RunEnded => Self::RunEnded,
            RunRefusal::RunRemoved => Self::RunRemoved,
            RunRefusal::InstanceMismatch => Self::InstanceMismatch,
            RunRefusal::Malformed => Self::Malformed,
        }
    }
}

impl WorkRefusal {
    pub fn code(&self) -> &'static str {
        match self.run_refusal() {
            Some(refusal) => refusal.code(),
            None => "job_not_of_run",
        }
    }

    pub fn message(&self) -> &'static str {
        match self.run_refusal() {
            Some(refusal) => refusal.message(),
            None => "The run does not work this job.",
        }
    }

    fn run_refusal(&self) -> Option<RunRefusal> {
        match self {
            Self::RunUnknown => Some(RunRefusal::RunUnknown),
            Self::RunNotStarted => Some(RunRefusal::RunNotStarted),
            Self::RunEnded => Some(RunRefusal::RunEnded),
            Self::RunRemoved => Some(RunRefusal::RunRemoved),
            Self::InstanceMismatch => Some(RunRefusal::InstanceMismatch),
            Self::Malformed => Some(RunRefusal::Malformed),
            Self::JobNotOfRun => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarkReadRefusal {
    Run(RunRefusal),
    ItemNotYours,
}

impl From<RunRefusal> for MarkReadRefusal {
    fn from(refusal: RunRefusal) -> Self {
        Self::Run(refusal)
    }
}

impl MarkReadRefusal {
    pub fn message(&self) -> &'static str {
        match self {
            Self::Run(refusal) => refusal.message(),
            Self::ItemNotYours => "An item is not at an address of the Run, nor a notice of it.",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct WorkNamed<'a> {
    pub job_id: Uuid,
    pub run_id: Uuid,
    pub instance_key: &'a str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkFacts {
    pub standing: RunStanding,
    pub job_of_run: Option<JobId>,
}

pub fn decide_work(named_job: JobId, facts: WorkFacts) -> Result<JobId, WorkRefusal> {
    let initial_job_id = match facts.standing {
        RunStanding::Unknown => return Err(WorkRefusal::RunUnknown),
        RunStanding::NotStarted => return Err(WorkRefusal::RunNotStarted),
        RunStanding::Ended => return Err(WorkRefusal::RunEnded),
        RunStanding::Removed => return Err(WorkRefusal::RunRemoved),
        RunStanding::StartedOnAnotherInstance => return Err(WorkRefusal::InstanceMismatch),
        RunStanding::Started { initial_job_id } => initial_job_id,
    };
    if facts.job_of_run == Some(named_job) {
        Ok(initial_job_id)
    } else {
        Err(WorkRefusal::JobNotOfRun)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixtures::job_id;

    #[test]
    fn the_run_is_checked_before_the_job() {
        let named = job_id();
        for (standing, expected) in [
            (RunStanding::Unknown, WorkRefusal::RunUnknown),
            (RunStanding::NotStarted, WorkRefusal::RunNotStarted),
            (RunStanding::Ended, WorkRefusal::RunEnded),
            (RunStanding::Removed, WorkRefusal::RunRemoved),
            (
                RunStanding::StartedOnAnotherInstance,
                WorkRefusal::InstanceMismatch,
            ),
        ] {
            let facts = WorkFacts {
                standing,
                job_of_run: Some(job_id()),
            };
            assert_eq!(decide_work(named, facts), Err(expected));
        }
    }

    #[test]
    fn a_live_run_works_only_the_job_it_works() {
        let tree = job_id();
        let job = job_id();
        let started = RunStanding::Started {
            initial_job_id: tree,
        };
        assert_eq!(
            decide_work(
                job,
                WorkFacts {
                    standing: started,
                    job_of_run: Some(job)
                }
            ),
            Ok(tree)
        );
        assert_eq!(
            decide_work(
                job,
                WorkFacts {
                    standing: started,
                    job_of_run: Some(job_id())
                }
            ),
            Err(WorkRefusal::JobNotOfRun)
        );
    }

    #[test]
    fn a_run_refusal_keeps_its_code_and_words_as_a_work_refusal() {
        for (run, work) in [
            (RunRefusal::RunUnknown, WorkRefusal::RunUnknown),
            (RunRefusal::RunNotStarted, WorkRefusal::RunNotStarted),
            (RunRefusal::RunEnded, WorkRefusal::RunEnded),
            (RunRefusal::RunRemoved, WorkRefusal::RunRemoved),
            (RunRefusal::InstanceMismatch, WorkRefusal::InstanceMismatch),
            (RunRefusal::Malformed, WorkRefusal::Malformed),
        ] {
            assert_eq!(WorkRefusal::from(run), work);
            assert_eq!(work.code(), run.code());
            assert_eq!(work.message(), run.message());
        }
        assert_eq!(WorkRefusal::JobNotOfRun.code(), "job_not_of_run");
    }
}
