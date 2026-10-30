use chrono::{DateTime, TimeDelta, Utc};

use crate::domain::ids::RunId;
use crate::domain::payload::CallMode;
use crate::domain::requester::ActsFor;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResultRefusal {
    NotSyncCall,
    ResultUnknown,
    NotAddressedToYou,
}

impl ResultRefusal {
    pub fn code(&self) -> &'static str {
        match self {
            Self::NotSyncCall => "not_sync_call",
            Self::ResultUnknown => "result_unknown",
            Self::NotAddressedToYou => "not_addressed_to_you",
        }
    }

    pub fn message(&self) -> &'static str {
        match self {
            Self::NotSyncCall => "Jobs keeps no Result for a Job that is not a synchronous call.",
            Self::ResultUnknown => "Jobs holds no such Result.",
            Self::NotAddressedToYou => "The Result is not addressed to this Run.",
        }
    }
}

pub const RESULT_RETENTION: TimeDelta = TimeDelta::days(7);

pub fn result_expires_at(kept_at: DateTime<Utc>, read_at: Option<DateTime<Utc>>) -> DateTime<Utc> {
    read_at.unwrap_or(kept_at) + RESULT_RETENTION
}

pub fn guard_result_alive(
    kept_at: DateTime<Utc>,
    read_at: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> Result<(), ResultRefusal> {
    if now < result_expires_at(kept_at, read_at) {
        Ok(())
    } else {
        Err(ResultRefusal::ResultUnknown)
    }
}

pub fn guard_keeps_a_result(mode: Option<CallMode>) -> Result<(), ResultRefusal> {
    match mode {
        Some(CallMode::Sync) => Ok(()),
        Some(CallMode::Background) | None => Err(ResultRefusal::NotSyncCall),
    }
}

pub fn guard_reads_result(
    addressed_to: RunId,
    reader: RunId,
    acts_for: &ActsFor,
) -> Result<(), ResultRefusal> {
    if addressed_to == reader || acts_for.includes(addressed_to) {
        Ok(())
    } else {
        Err(ResultRefusal::NotAddressedToYou)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixtures::run_id;

    #[test]
    fn a_result_nobody_reads_lives_seven_days_from_the_moment_it_was_kept() {
        let kept = chrono::DateTime::<Utc>::UNIX_EPOCH;
        let last_moment = kept + RESULT_RETENTION - TimeDelta::seconds(1);
        assert_eq!(guard_result_alive(kept, None, last_moment), Ok(()));
        assert_eq!(
            guard_result_alive(kept, None, kept + RESULT_RETENTION),
            Err(ResultRefusal::ResultUnknown)
        );
    }

    #[test]
    fn a_result_read_with_keep_lives_seven_days_from_that_read() {
        let kept = chrono::DateTime::<Utc>::UNIX_EPOCH;
        let read = kept + TimeDelta::days(5);
        assert_eq!(
            guard_result_alive(kept, Some(read), kept + TimeDelta::days(11)),
            Ok(())
        );
        assert_eq!(
            guard_result_alive(kept, Some(read), read + RESULT_RETENTION),
            Err(ResultRefusal::ResultUnknown)
        );
    }

    #[test]
    fn only_a_synchronous_call_keeps_a_result() {
        assert_eq!(guard_keeps_a_result(Some(CallMode::Sync)), Ok(()));
        assert_eq!(
            guard_keeps_a_result(Some(CallMode::Background)),
            Err(ResultRefusal::NotSyncCall)
        );
        assert_eq!(guard_keeps_a_result(None), Err(ResultRefusal::NotSyncCall));
    }

    #[test]
    fn only_the_run_it_is_addressed_to_or_a_run_acting_for_it_reads_a_result() {
        let addressed = run_id();
        assert_eq!(
            guard_reads_result(addressed, addressed, &ActsFor::default()),
            Ok(())
        );
        assert_eq!(
            guard_reads_result(addressed, run_id(), &ActsFor::default()),
            Err(ResultRefusal::NotAddressedToYou)
        );
        assert_eq!(
            guard_reads_result(addressed, run_id(), &ActsFor::new([addressed])),
            Ok(())
        );
    }
}
