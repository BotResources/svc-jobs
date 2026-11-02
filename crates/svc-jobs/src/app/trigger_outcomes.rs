use metrics::{counter, describe_counter};

pub const TRIGGER_TERMINATIONS_TOTAL: &str = "jobs_trigger_terminations_total";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    RunFailed,
    Ignored,
    NoRun,
    StoreFailed,
    Unreadable,
    Unsettled,
}

impl Outcome {
    const ALL: [Self; 6] = [
        Self::RunFailed,
        Self::Ignored,
        Self::NoRun,
        Self::StoreFailed,
        Self::Unreadable,
        Self::Unsettled,
    ];

    pub fn label(self) -> &'static str {
        match self {
            Self::RunFailed => "run_failed",
            Self::Ignored => "ignored",
            Self::NoRun => "no_run",
            Self::StoreFailed => "store_failed",
            Self::Unreadable => "unreadable",
            Self::Unsettled => "unsettled",
        }
    }
}

pub fn describe() {
    describe_counter!(
        TRIGGER_TERMINATIONS_TOTAL,
        "Trigger terminations and trigger stores this service handled, by outcome: run_failed (the \
         pending run was failed), ignored (the run had started, was reserved or had ended), \
         no_run (the sequence names no run this service dispatched, after the lookup window), \
         store_failed (the stream sequence of a dispatched trigger could not be stored, so its \
         termination will fail nothing), unreadable (an advisory without a readable sequence) and \
         unsettled (the run could not be failed; the advisory is not delivered again). The last \
         three, and no_run for a trigger this service did dispatch, are runs left pending"
    );
    for outcome in Outcome::ALL {
        counter!(TRIGGER_TERMINATIONS_TOTAL, "outcome" => outcome.label()).absolute(0);
    }
}

pub fn record(outcome: Outcome) {
    counter!(TRIGGER_TERMINATIONS_TOTAL, "outcome" => outcome.label()).increment(1);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_outcome_has_its_own_label() {
        let labels: std::collections::HashSet<_> =
            Outcome::ALL.iter().map(|outcome| outcome.label()).collect();
        assert_eq!(labels.len(), Outcome::ALL.len());
    }
}
