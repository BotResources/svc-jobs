pub mod failure;
pub mod origin;
pub mod parts;
pub mod plan;
pub mod progression;
pub mod status;
pub mod step;

use chrono::{DateTime, TimeDelta, Utc};

use crate::domain::attempts::AttemptNumber;
use crate::domain::ids::{RetryScheduleId, RunId};
use crate::domain::run::origin::RunOrigin;
use crate::domain::run::parts::{
    RetrySchedule, RunCancellationRequest, RunStart, RunTerminal, RunnerInstanceReference,
};
use crate::domain::run::plan::RunPlan;
use crate::domain::run::progression::RunProgression;
use crate::domain::run::status::{RunStatus, RunTerminalKind};
use crate::domain::run::step::Step;
use crate::error::JobsError;

#[derive(Debug, Clone)]
pub struct RunState {
    pub id: RunId,
    pub attempt_number: AttemptNumber,
    pub dispatched_at: DateTime<Utc>,
    pub automatic_retry_schedule_id: Option<RetryScheduleId>,
    pub start: Option<RunStart>,
    pub terminal: Option<RunTerminal>,
    pub plan: Option<RunPlan>,
    pub steps: Vec<Step>,
    pub retry_schedule: Option<RetrySchedule>,
    pub cancellation_request: Option<RunCancellationRequest>,
}

#[derive(Debug, Clone)]
pub struct Run {
    id: RunId,
    attempt_number: AttemptNumber,
    dispatched_at: DateTime<Utc>,
    automatic_retry_schedule_id: Option<RetryScheduleId>,
    start: Option<RunStart>,
    terminal: Option<RunTerminal>,
    plan: Option<RunPlan>,
    steps: Vec<Step>,
    retry_schedule: Option<RetrySchedule>,
    cancellation_request: Option<RunCancellationRequest>,
}

fn corrupt(reason_code: &'static str) -> JobsError {
    JobsError::CorruptState { reason_code }
}

impl Run {
    pub fn hydrate(state: RunState) -> Result<Self, JobsError> {
        if state.attempt_number.is_first() != state.automatic_retry_schedule_id.is_none() {
            return Err(corrupt("retry_schedule_does_not_match_attempt_number"));
        }
        if let Some(start) = &state.start
            && start.started_at() < state.dispatched_at
        {
            return Err(corrupt("run_started_before_dispatch"));
        }
        if let Some(terminal) = &state.terminal {
            if terminal.occurred_at() < state.dispatched_at {
                return Err(corrupt("run_finished_before_dispatch"));
            }
            match &state.start {
                Some(start) if terminal.occurred_at() < start.started_at() => {
                    return Err(corrupt("run_finished_before_start"));
                }
                None if terminal.kind() != RunTerminalKind::Cancelled => {
                    return Err(corrupt("run_settled_without_start"));
                }
                _ => {}
            }
        }
        let mut steps = state.steps;
        steps.sort_by_key(Step::index);
        if steps
            .windows(2)
            .any(|pair| pair[0].index() == pair[1].index())
        {
            return Err(corrupt("duplicate_step_index"));
        }
        if state.retry_schedule.is_some() && !transiently_failed(state.terminal.as_ref()) {
            return Err(corrupt("retry_scheduled_without_transient_failure"));
        }
        Ok(Self {
            id: state.id,
            attempt_number: state.attempt_number,
            dispatched_at: state.dispatched_at,
            automatic_retry_schedule_id: state.automatic_retry_schedule_id,
            start: state.start,
            terminal: state.terminal,
            plan: state.plan,
            steps,
            retry_schedule: state.retry_schedule,
            cancellation_request: state.cancellation_request,
        })
    }

    pub fn dispatched(
        id: RunId,
        attempt_number: AttemptNumber,
        dispatched_at: DateTime<Utc>,
        automatic_retry_schedule_id: Option<RetryScheduleId>,
    ) -> Result<Self, JobsError> {
        Self::hydrate(RunState {
            id,
            attempt_number,
            dispatched_at,
            automatic_retry_schedule_id,
            start: None,
            terminal: None,
            plan: None,
            steps: vec![],
            retry_schedule: None,
            cancellation_request: None,
        })
    }

    pub fn id(&self) -> RunId {
        self.id
    }

    pub fn attempt_number(&self) -> AttemptNumber {
        self.attempt_number
    }

    pub fn dispatched_at(&self) -> DateTime<Utc> {
        self.dispatched_at
    }

    pub fn automatic_retry_schedule_id(&self) -> Option<RetryScheduleId> {
        self.automatic_retry_schedule_id
    }

    pub fn start(&self) -> Option<&RunStart> {
        self.start.as_ref()
    }

    pub fn terminal(&self) -> Option<&RunTerminal> {
        self.terminal.as_ref()
    }

    pub fn plan(&self) -> Option<&RunPlan> {
        self.plan.as_ref()
    }

    pub fn steps(&self) -> &[Step] {
        &self.steps
    }

    pub fn retry_schedule(&self) -> Option<RetrySchedule> {
        self.retry_schedule
    }

    pub fn cancellation_request(&self) -> Option<&RunCancellationRequest> {
        self.cancellation_request.as_ref()
    }

    pub fn status(&self) -> RunStatus {
        match (&self.terminal, &self.start) {
            (Some(terminal), _) => terminal.kind().as_status(),
            (None, Some(_)) => RunStatus::Started,
            (None, None) => RunStatus::Pending,
        }
    }

    pub fn is_terminal(&self) -> bool {
        self.terminal.is_some()
    }

    pub fn has_started(&self) -> bool {
        self.start.is_some()
    }

    pub fn origin(&self, job_has_predecessor: bool) -> RunOrigin {
        RunOrigin::derive(self.attempt_number.is_first(), job_has_predecessor)
    }

    pub fn current_step(&self) -> Option<&Step> {
        self.steps.last()
    }

    pub fn progression(&self) -> Option<RunProgression<'_>> {
        RunProgression::of(self.plan.as_ref(), self.current_step())
    }

    pub fn is_executed_by(&self, instance: &RunnerInstanceReference) -> bool {
        self.start
            .as_ref()
            .is_some_and(|start| start.instance() == instance)
    }

    pub fn last_activity_at(&self) -> DateTime<Utc> {
        let mut latest = self.dispatched_at;
        if let Some(start) = &self.start {
            latest = latest.max(start.started_at());
        }
        if let Some(step) = self.current_step() {
            latest = latest.max(step.started_at());
        }
        if let Some(terminal) = &self.terminal {
            latest = latest.max(terminal.occurred_at());
        }
        latest
    }

    pub fn has_outrun(&self, now: DateTime<Utc>, max_duration: TimeDelta) -> bool {
        match (&self.terminal, &self.start) {
            (None, Some(start)) => now - start.started_at() > max_duration,
            _ => false,
        }
    }
}

fn transiently_failed(terminal: Option<&RunTerminal>) -> bool {
    terminal.is_some_and(|terminal| {
        terminal.kind() == RunTerminalKind::Failed
            && terminal
                .failure()
                .is_some_and(|report| report.kind().is_retryable())
    })
}

#[cfg(test)]
mod tests;
