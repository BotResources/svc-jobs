pub mod failure;
mod hydration;
pub mod origin;
pub mod parts;
pub mod plan;
pub mod progression;
pub mod start_refusal;
pub mod start_standing;
pub mod status;
pub mod step;

use chrono::{DateTime, TimeDelta, Utc};

use crate::domain::attempts::AttemptNumber;
use crate::domain::fleet::run_duration::MaxRunDuration;
use crate::domain::ids::{RetryScheduleId, RunId};
use crate::domain::run::origin::RunOrigin;
use crate::domain::run::parts::{
    RetrySchedule, RunCancellationRequest, RunReservation, RunStart, RunTerminal,
    RunnerInstanceReference,
};
use crate::domain::run::plan::RunPlan;
use crate::domain::run::progression::RunProgression;
use crate::domain::run::status::RunStatus;
use crate::domain::run::step::Step;
use crate::error::JobsError;

#[derive(Debug, Clone)]
pub struct RunState {
    pub id: RunId,
    pub attempt_number: AttemptNumber,
    pub dispatched_at: DateTime<Utc>,
    pub automatic_retry_schedule_id: Option<RetryScheduleId>,
    pub max_duration: Option<MaxRunDuration>,
    pub reservation: Option<RunReservation>,
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
    max_duration: Option<MaxRunDuration>,
    reservation: Option<RunReservation>,
    start: Option<RunStart>,
    terminal: Option<RunTerminal>,
    plan: Option<RunPlan>,
    steps: Vec<Step>,
    retry_schedule: Option<RetrySchedule>,
    cancellation_request: Option<RunCancellationRequest>,
}

impl Run {
    pub fn hydrate(state: RunState) -> Result<Self, JobsError> {
        hydration::check_timeline(&state)?;
        let mut steps = state.steps;
        steps.sort_by_key(Step::index);
        if steps
            .windows(2)
            .any(|pair| pair[0].index() == pair[1].index())
        {
            return Err(hydration::corrupt("duplicate_step_index"));
        }
        if state.retry_schedule.is_some() && !hydration::transiently_failed(state.terminal.as_ref())
        {
            return Err(hydration::corrupt(
                "retry_scheduled_without_transient_failure",
            ));
        }
        Ok(Self {
            id: state.id,
            attempt_number: state.attempt_number,
            dispatched_at: state.dispatched_at,
            automatic_retry_schedule_id: state.automatic_retry_schedule_id,
            max_duration: state.max_duration,
            reservation: state.reservation,
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
            max_duration: None,
            reservation: None,
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

    pub fn max_duration(&self) -> Option<MaxRunDuration> {
        self.max_duration
    }

    pub fn reservation(&self) -> Option<&RunReservation> {
        self.reservation.as_ref()
    }

    pub fn is_reserved(&self) -> bool {
        self.reservation.is_some()
    }

    pub fn instance(&self) -> Option<&RunnerInstanceReference> {
        self.start
            .as_ref()
            .map(RunStart::instance)
            .or_else(|| self.reservation.as_ref().map(RunReservation::instance))
    }

    pub fn is_held_by_an_instance(&self) -> bool {
        self.has_started() || self.is_reserved()
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

    pub fn is_held_by(&self, instance: &RunnerInstanceReference) -> bool {
        self.instance() == Some(instance)
    }

    pub fn has_lost_its_claim(&self, now: DateTime<Utc>, reservation_delay: TimeDelta) -> bool {
        match (&self.reservation, &self.start, &self.terminal) {
            (Some(reservation), None, None) => now - reservation.reserved_at() > reservation_delay,
            _ => false,
        }
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

    pub fn has_a_stranded_trigger(
        &self,
        now: DateTime<Utc>,
        delay: TimeDelta,
        claimed_since: DateTime<Utc>,
    ) -> bool {
        self.reservation.is_none()
            && self.start.is_none()
            && self.terminal.is_none()
            && now - self.dispatched_at.max(claimed_since) > delay
    }

    pub fn counts_as_attempt(&self) -> bool {
        self.terminal
            .as_ref()
            .is_none_or(|terminal| terminal.withdrawal().is_none())
    }

    pub fn has_outrun(&self, now: DateTime<Utc>, service_default: TimeDelta) -> bool {
        let max_duration = self
            .max_duration
            .map_or(service_default, MaxRunDuration::as_delta);
        match (&self.terminal, &self.start) {
            (None, Some(start)) => now - start.started_at() > max_duration,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests;
