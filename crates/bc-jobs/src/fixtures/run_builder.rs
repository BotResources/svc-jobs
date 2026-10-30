use super::*;

pub struct RunBuilder {
    state: RunState,
}

impl RunBuilder {
    pub fn new(attempt: u32) -> Self {
        let attempt_number = AttemptNumber::new(attempt).expect("positive attempt");
        Self {
            state: RunState {
                id: run_id(),
                attempt_number,
                dispatched_at: ts(0),
                automatic_retry_schedule_id: (!attempt_number.is_first()).then(schedule_id),
                max_duration: None,
                reservation: None,
                start: None,
                terminal: None,
                plan: None,
                steps: vec![],
                retry_schedule: None,
                cancellation_request: None,
            },
        }
    }

    pub fn retry_of_schedule(mut self, id: RetryScheduleId) -> Self {
        self.state.automatic_retry_schedule_id = Some(id);
        self
    }

    pub fn started(self, at: DateTime<Utc>) -> Self {
        self.started_on(instance(), at)
    }

    pub fn reserved(self, at: DateTime<Utc>) -> Self {
        self.reserved_on(instance(), at)
    }

    pub fn reserved_on(mut self, instance: RunnerInstanceReference, at: DateTime<Utc>) -> Self {
        self.state.reservation = Some(RunReservation::new(instance, claim_id(), at));
        self
    }

    pub fn with_max_duration(mut self, seconds: i64) -> Self {
        self.state.max_duration =
            Some(MaxRunDuration::from_seconds(seconds).expect("positive duration"));
        self
    }

    pub fn started_on(mut self, instance: RunnerInstanceReference, at: DateTime<Utc>) -> Self {
        self.state.start = Some(RunStart::new(instance, at));
        self
    }

    pub fn completed(mut self, at: DateTime<Utc>) -> Self {
        self.state.terminal = Some(RunTerminal::completed(at));
        self
    }

    pub fn cancelled(mut self, at: DateTime<Utc>) -> Self {
        self.state.terminal = Some(RunTerminal::cancelled(at));
        self
    }

    pub fn withdrawn(mut self, at: DateTime<Utc>, reason: &str) -> Self {
        self.state.terminal = Some(RunTerminal::withdrawn(
            at,
            crate::domain::keys::ReasonCode::new(reason).unwrap(),
        ));
        self
    }

    pub fn failed(self, at: DateTime<Utc>, kind: RunFailureKind) -> Self {
        self.failed_with(at, report(kind))
    }

    pub fn failed_with(mut self, at: DateTime<Utc>, failure: RunFailureReport) -> Self {
        self.state.terminal = Some(RunTerminal::failed(at, failure));
        self
    }

    pub fn retry_due(mut self, at: DateTime<Utc>) -> Self {
        self.state.retry_schedule = Some(RetrySchedule::new(schedule_id(), at));
        self
    }

    pub fn with_plan(mut self, plan: RunPlan) -> Self {
        self.state.plan = Some(plan);
        self
    }

    pub fn with_step(mut self, index: u32, label: &str, at: DateTime<Utc>) -> Self {
        self.state.steps.push(Step::new(
            StepIndex::new(index),
            StepLabel::new(label).expect("valid label"),
            at,
        ));
        self
    }

    pub fn build(self) -> Run {
        Run::hydrate(self.state).expect("fixture run is valid")
    }
}
