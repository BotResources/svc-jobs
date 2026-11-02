mod answer;
mod facts;
#[cfg(test)]
mod tests;

pub use facts::{
    AnswerRecord, AutoFinishFacts, AutoFinishWait, CallerRun, CallerRunEnd, FinishPremise,
};

use crate::commands::job::resolution::FinishJob;
use crate::domain::ids::{JobId, ResolutionId, RunId};
use crate::domain::job::Job;
use crate::domain::ownership::{FinishVerdict, ResolutionRequester};
use crate::domain::requester::Requester;
use crate::domain::run::Run;
use crate::domain::run::status::RunTerminalKind;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinishByRule {
    verdict: FinishVerdict,
    called: JobId,
    restart_jobs: Vec<JobId>,
    premise: FinishPremise,
}

impl FinishByRule {
    pub fn called(&self) -> JobId {
        self.called
    }

    pub fn restart_jobs_to_finish(&self) -> &[JobId] {
        &self.restart_jobs
    }

    pub fn premise(&self) -> &FinishPremise {
        &self.premise
    }

    pub fn finish_command(&self, resolution_id: ResolutionId) -> FinishJob {
        FinishJob {
            resolution_id,
            requester: ResolutionRequester::Rule(self.verdict.clone()),
        }
    }
}

struct LatestRun<'a> {
    run: &'a Run,
    of_a_restart_job: bool,
}

impl AutoFinishFacts {
    pub fn decide(&self) -> Result<FinishByRule, AutoFinishWait> {
        let called = &self.called;
        let (Requester::Run(caller_run), Some(call)) = (called.requester(), called.call()) else {
            return Err(AutoFinishWait::NotCalledByARun);
        };
        if called.is_terminal() || called.is_deleted() {
            return Err(AutoFinishWait::AlreadySettled);
        }
        if self.caller_run_failed(*caller_run) {
            return Err(AutoFinishWait::CallerDidNotSucceed);
        }
        if self.restart_jobs.iter().any(restart_job_is_pending) {
            return Err(AutoFinishWait::RestartJobPending);
        }
        let latest = self
            .latest_run()
            .filter(|latest| {
                latest
                    .run
                    .terminal()
                    .is_some_and(|terminal| terminal.kind() == RunTerminalKind::Completed)
            })
            .ok_or(AutoFinishWait::LatestRunNotCompleted)?;
        if !self.restarts_due.is_empty() {
            return Err(AutoFinishWait::RestartDue);
        }
        let read_at = answer::read_at(
            call.mode,
            &self.answers,
            latest.run.id(),
            latest.of_a_restart_job,
        )
        .ok_or(AutoFinishWait::AnswerNotRead)?;
        if !self.a_caller_run_succeeded_since(read_at) {
            return Err(AutoFinishWait::CallerRunNotEnded);
        }
        Ok(self.plan())
    }

    fn caller_run_failed(&self, caller_run: RunId) -> bool {
        self.caller_runs.iter().any(|run| {
            run.run_id == caller_run
                && run
                    .ended
                    .is_some_and(|end| end.kind != RunTerminalKind::Completed)
        })
    }

    fn a_caller_run_succeeded_since(&self, read_at: chrono::DateTime<chrono::Utc>) -> bool {
        self.caller_runs.iter().any(|run| {
            run.ended
                .is_some_and(|end| end.kind == RunTerminalKind::Completed && end.at >= read_at)
        })
    }

    fn latest_run(&self) -> Option<LatestRun<'_>> {
        let own = self.called.runs().iter().map(|run| LatestRun {
            run,
            of_a_restart_job: false,
        });
        let restarted = self
            .restart_jobs
            .iter()
            .flat_map(Job::runs)
            .filter(|run| {
                !run.terminal()
                    .is_some_and(|terminal| terminal.kind() != RunTerminalKind::Completed)
            })
            .map(|run| LatestRun {
                run,
                of_a_restart_job: true,
            });
        own.chain(restarted)
            .max_by_key(|latest| (latest.run.dispatched_at(), latest.run.id()))
    }

    fn plan(&self) -> FinishByRule {
        let mut family: Vec<JobId> = self.restart_jobs.iter().map(Job::id).collect();
        family.sort_by_key(|job| job.as_uuid());
        let mut due = self.restarts_due.clone();
        due.sort_by_key(|run| run.as_uuid());
        let mut to_finish: Vec<JobId> = self
            .restart_jobs
            .iter()
            .filter(|job| !job.is_terminal())
            .map(Job::id)
            .collect();
        to_finish.sort_by_key(|job| job.as_uuid());
        FinishByRule {
            verdict: FinishVerdict::granted(),
            called: self.called.id(),
            restart_jobs: to_finish,
            premise: FinishPremise {
                restart_jobs: family,
                restarts_due: due,
            },
        }
    }
}

fn restart_job_is_pending(job: &Job) -> bool {
    !job.is_terminal() && (job.is_waiting() || job.active_run().is_some())
}
