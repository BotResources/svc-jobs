use crate::domain::attempts::AttemptNumber;
use crate::domain::ids::RunId;
use crate::domain::job::Job;
use crate::error::JobsError;

/// What a claim repeated under the identity of an earlier one is answered
/// with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecalledClaim {
    /// The Run reserved to that claim, not yet started: the reply was lost,
    /// and is sent again.
    Pending {
        run_id: RunId,
        attempt: AttemptNumber,
    },
    /// The Run was started or settled: the identity named one slot, and that
    /// slot was spent. The Instance mints a new identity for a new slot.
    Spent,
}

impl Job {
    pub fn recall_claim(&self, run_id: RunId) -> Result<RecalledClaim, JobsError> {
        let run = self.find_run(run_id)?;
        if !run.is_reserved() {
            return Err(JobsError::RunNotReserved {
                run_id: run_id.as_uuid(),
            });
        }
        if run.has_started() || run.is_terminal() {
            return Ok(RecalledClaim::Spent);
        }
        Ok(RecalledClaim::Pending {
            run_id,
            attempt: run.attempt_number(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::run::failure::RunFailureKind;
    use crate::fixtures::{JobBuilder, RunBuilder, ts};

    #[test]
    fn a_reserved_unstarted_run_is_handed_back_to_its_claim() {
        // Given: a run reserved by a claim and not started
        let run = RunBuilder::new(1).reserved(ts(1)).build();
        let run_id = run.id();
        let job = JobBuilder::new().with_run(run).build();
        // When/Then: the repeated claim gets it again
        assert_eq!(
            job.recall_claim(run_id),
            Ok(RecalledClaim::Pending {
                run_id,
                attempt: AttemptNumber::FIRST
            })
        );
    }

    #[test]
    fn a_claim_whose_run_started_or_settled_was_spent() {
        // Given: a reserved run its instance started, and one that lost its claim
        let started = RunBuilder::new(1).reserved(ts(1)).started(ts(2)).build();
        let started_id = started.id();
        let lost = RunBuilder::new(1)
            .reserved(ts(1))
            .failed(ts(90), RunFailureKind::Transient)
            .build();
        let lost_id = lost.id();
        // When/Then: both identities are spent
        assert_eq!(
            JobBuilder::new()
                .with_run(started)
                .build()
                .recall_claim(started_id),
            Ok(RecalledClaim::Spent)
        );
        assert_eq!(
            JobBuilder::new()
                .with_run(lost)
                .build()
                .recall_claim(lost_id),
            Ok(RecalledClaim::Spent)
        );
    }
}
