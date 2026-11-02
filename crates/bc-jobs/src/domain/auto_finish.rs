use chrono::{DateTime, Utc};

use crate::domain::ids::{JobId, RunId};
use crate::domain::job::Job;
use crate::domain::run::status::RunTerminalKind;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AnswerRecord {
    pub run_id: RunId,
    pub recorded_at: DateTime<Utc>,
    pub read_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CallerRunEnd {
    pub kind: RunTerminalKind,
    pub at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CallerRun {
    pub run_id: RunId,
    pub ended: Option<CallerRunEnd>,
}

#[derive(Debug, Clone)]
pub struct AutoFinishFacts {
    pub called: Job,
    pub restart_jobs: Vec<Job>,
    pub restarts_due: Vec<RunId>,
    pub answers: Vec<AnswerRecord>,
    pub caller_runs: Vec<CallerRun>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinishPremise {
    pub restart_jobs: Vec<JobId>,
    pub restarts_due: Vec<RunId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutoFinishWait {
    NotCalledByARun,
    AlreadySettled,
    CallerDidNotSucceed,
    LatestRunNotCompleted,
    RestartJobPending,
    RestartDue,
    AnswerNotRead,
    CallerRunNotEnded,
}
