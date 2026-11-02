use crate::domain::run::Run;
use crate::domain::run::parts::{RunStart, RunnerInstanceReference};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartStanding<'a> {
    Ended,
    HeldBy(&'a RunStart),
    ReservedFor(&'a RunnerInstanceReference),
    Pending,
}

impl Run {
    pub fn start_standing(&self) -> StartStanding<'_> {
        if self.is_terminal() {
            return StartStanding::Ended;
        }
        if let Some(start) = self.start() {
            return StartStanding::HeldBy(start);
        }
        match self.reservation() {
            Some(reservation) => StartStanding::ReservedFor(reservation.instance()),
            None => StartStanding::Pending,
        }
    }
}

#[cfg(test)]
mod tests;
