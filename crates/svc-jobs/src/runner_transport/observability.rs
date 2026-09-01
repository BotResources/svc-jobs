//! What the service publishes about the runner facts it could not accept.
//!
//! A fact the transport cannot read is acknowledged and dropped: holding it
//! would block the stream behind a frame no redelivery can fix. But the drop is
//! a loss — a terminal fact dropped here is a run whose end nobody ever
//! recorded, and the job waits for a backstop hours away. A warning line is not
//! an answer to that: nothing alerts on it and nothing counts it. The counter
//! below is the first-class signal.

use contract_jobs::runner_transport::RunnerStatusFact;
use metrics::{counter, describe_counter};

pub const FACTS_DISCARDED_TOTAL: &str = "jobs_runner_facts_discarded_total";

const LOG: &str = "log";
const OTHER: &str = "other";

/// What a dropped frame is counted as.
///
/// It is the identity the consumer **already resolved** when it dispatched the
/// frame, handed down rather than read off the subject a second time: a subject
/// carries any number of segments, so a second reading is free to disagree with
/// the first — and a terminal fact dropped under `other` is invisible to the
/// alert built on `fact="failed"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Frame {
    Status(RunnerStatusFact),
    Log,
    Unrecognized,
}

impl Frame {
    fn label(self) -> &'static str {
        match self {
            Self::Status(fact) => fact.as_str(),
            Self::Log => LOG,
            Self::Unrecognized => OTHER,
        }
    }
}

/// Called once, where the transport tasks are spawned — never from a consumer,
/// which restarts under supervision and would re-describe on every restart.
pub fn describe() {
    describe_counter!(
        FACTS_DISCARDED_TOTAL,
        "Runner facts this service could not read, or refused before they reached their job, \
         acknowledged and dropped, by fact. A fact the domain examined and declined — a late \
         fact on an already-terminal job — is not counted here: it was understood, not lost"
    );
    // A series that has never fired must still exist, or the alert on the two
    // terminal facts stays silent in exactly the case it is written for.
    for terminal in [RunnerStatusFact::Completed, RunnerStatusFact::Failed] {
        counter!(FACTS_DISCARDED_TOTAL, "fact" => terminal.as_str()).absolute(0);
    }
}

pub fn discarded(frame: Frame) {
    counter!(FACTS_DISCARDED_TOTAL, "fact" => frame.label()).increment(1);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_dropped_status_fact_is_counted_under_the_word_its_subject_carries() {
        // Given: every fact of the transport, as the consumer resolved it
        for fact in RunnerStatusFact::ALL {
            // When/Then: the series is named after the fact itself, so an operator alerting on
            // `fact="failed"` is alerting on dropped terminal facts and nothing else
            assert_eq!(Frame::Status(fact).label(), fact.as_str());
        }
    }

    #[test]
    fn the_two_other_frames_carry_bounded_labels_of_their_own() {
        // Given: a log line and a frame the consumer could not identify at all
        // When/Then: neither borrows a status fact's series, and neither can mint one — no
        // wire value ever reaches a label
        assert_eq!(Frame::Log.label(), LOG);
        assert_eq!(Frame::Unrecognized.label(), OTHER);
    }
}
