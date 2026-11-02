use crate::domain::run::RunState;
use crate::domain::run::failure::{PASSPORT_REQUIRED, TRIGGER_TERMINATED};
use crate::domain::run::parts::RunTerminal;
use crate::domain::run::status::RunTerminalKind;
use crate::error::JobsError;

pub(super) fn corrupt(reason_code: &'static str) -> JobsError {
    JobsError::CorruptState { reason_code }
}

pub(super) fn check_timeline(state: &RunState) -> Result<(), JobsError> {
    if state.attempt_number.is_first() != state.automatic_retry_schedule_id.is_none() {
        return Err(corrupt("retry_schedule_does_not_match_attempt_number"));
    }
    if let Some(start) = &state.start
        && start.started_at() < state.dispatched_at
    {
        return Err(corrupt("run_started_before_dispatch"));
    }
    if let Some(reservation) = &state.reservation {
        if reservation.reserved_at() < state.dispatched_at {
            return Err(corrupt("run_reserved_before_dispatch"));
        }
        if let Some(start) = &state.start
            && start.instance() != reservation.instance()
        {
            return Err(corrupt("run_started_by_another_instance_than_its_claimant"));
        }
    }
    if let Some(terminal) = &state.terminal {
        if terminal.occurred_at() < state.dispatched_at {
            return Err(corrupt("run_finished_before_dispatch"));
        }
        match &state.start {
            Some(start) if terminal.occurred_at() < start.started_at() => {
                return Err(corrupt("run_finished_before_start"));
            }
            None if !settles_unstarted(terminal, state.reservation.is_some()) => {
                return Err(corrupt("run_settled_without_start"));
            }
            _ => {}
        }
        if terminal.withdrawal().is_some() && (state.start.is_some() || state.reservation.is_some())
        {
            return Err(corrupt("withdrawn_run_was_taken"));
        }
    }
    Ok(())
}

fn settles_unstarted(terminal: &RunTerminal, reserved: bool) -> bool {
    match terminal.kind() {
        RunTerminalKind::Cancelled => true,
        RunTerminalKind::Failed => reserved || fails_before_any_start(terminal),
        RunTerminalKind::Completed => false,
    }
}

fn fails_before_any_start(terminal: &RunTerminal) -> bool {
    terminal.failure().is_some_and(|report| {
        matches!(
            report.reason_code().as_str(),
            PASSPORT_REQUIRED | TRIGGER_TERMINATED
        )
    })
}

pub(super) fn transiently_failed(terminal: Option<&RunTerminal>) -> bool {
    terminal.is_some_and(|terminal| {
        terminal.kind() == RunTerminalKind::Failed
            && terminal
                .failure()
                .is_some_and(|report| report.kind().is_retryable())
    })
}
