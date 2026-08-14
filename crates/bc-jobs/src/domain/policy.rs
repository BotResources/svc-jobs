use chrono::{DateTime, TimeDelta, Utc};

use crate::domain::attempts::{AttemptNumber, MaxAttempts};
use crate::error::JobsError;

const BASIS: i64 = 10_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Jitter(i64);

impl Jitter {
    pub const MIDPOINT: Self = Self(BASIS / 2);

    pub fn from_basis_points(value: i64) -> Result<Self, JobsError> {
        if (0..=BASIS).contains(&value) {
            Ok(Self(value))
        } else {
            Err(JobsError::InvalidJitter)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServiceLimits {
    pub max_attempts_ceiling: u32,
    pub default_max_attempts: MaxAttempts,
    pub max_run_duration: TimeDelta,
    pub inactivity_timeout: TimeDelta,
}

impl Default for ServiceLimits {
    fn default() -> Self {
        Self {
            max_attempts_ceiling: 10,
            default_max_attempts: MaxAttempts::new(3).expect("three is a positive attempt budget"),
            max_run_duration: TimeDelta::hours(72),
            inactivity_timeout: TimeDelta::hours(24),
        }
    }
}

impl ServiceLimits {
    pub fn budget(&self, declared: Option<MaxAttempts>) -> MaxAttempts {
        declared.unwrap_or(self.default_max_attempts)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    pub base_delay: TimeDelta,
    pub factor: u32,
    pub max_delay: TimeDelta,
    pub jitter_span_basis_points: i64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            base_delay: TimeDelta::seconds(10),
            factor: 3,
            max_delay: TimeDelta::hours(1),
            jitter_span_basis_points: 2_000,
        }
    }
}

impl RetryPolicy {
    pub fn backoff_for(&self, failed_attempt: AttemptNumber) -> TimeDelta {
        let exponent = failed_attempt.get().saturating_sub(1);
        let mut milliseconds = self.base_delay.num_milliseconds();
        for _ in 0..exponent {
            milliseconds = milliseconds.saturating_mul(i64::from(self.factor));
            if milliseconds >= self.max_delay.num_milliseconds() {
                return self.max_delay;
            }
        }
        TimeDelta::milliseconds(milliseconds.min(self.max_delay.num_milliseconds()))
    }

    pub fn due_at(
        &self,
        failed_at: DateTime<Utc>,
        failed_attempt: AttemptNumber,
        jitter: Jitter,
        retry_after_hint: Option<TimeDelta>,
    ) -> DateTime<Utc> {
        let backoff = self.backoff_for(failed_attempt).num_milliseconds();
        let span = backoff.saturating_mul(self.jitter_span_basis_points) / BASIS;
        let jittered = backoff - span + (span.saturating_mul(2).saturating_mul(jitter.0) / BASIS);
        let hint = retry_after_hint.map_or(0, |hint| hint.num_milliseconds());
        failed_at + TimeDelta::milliseconds(jittered.max(hint))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at() -> DateTime<Utc> {
        DateTime::from_timestamp(1_700_000_000, 0).unwrap()
    }

    #[test]
    fn backoff_grows_exponentially_and_stops_at_the_ceiling() {
        // Given: the platform retry policy
        let policy = RetryPolicy::default();
        // When: successive attempts fail
        // Then: the delay multiplies by the factor until the maximum caps it
        assert_eq!(
            policy.backoff_for(AttemptNumber::new(1).unwrap()),
            TimeDelta::seconds(10)
        );
        assert_eq!(
            policy.backoff_for(AttemptNumber::new(2).unwrap()),
            TimeDelta::seconds(30)
        );
        assert_eq!(
            policy.backoff_for(AttemptNumber::new(9).unwrap()),
            TimeDelta::hours(1)
        );
    }

    #[test]
    fn jitter_spreads_the_delay_around_the_backoff() {
        // Given: a policy with a twenty percent jitter span
        let policy = RetryPolicy::default();
        let attempt = AttemptNumber::new(1).unwrap();
        // When: the extremes and the midpoint of the jitter range are drawn
        let low = policy.due_at(at(), attempt, Jitter::from_basis_points(0).unwrap(), None);
        let mid = policy.due_at(at(), attempt, Jitter::MIDPOINT, None);
        let high = policy.due_at(
            at(),
            attempt,
            Jitter::from_basis_points(10_000).unwrap(),
            None,
        );
        // Then: the due time lands within eight to twelve seconds, centred on ten
        assert_eq!(low, at() + TimeDelta::seconds(8));
        assert_eq!(mid, at() + TimeDelta::seconds(10));
        assert_eq!(high, at() + TimeDelta::seconds(12));
    }

    #[test]
    fn a_runner_hint_may_lengthen_the_delay() {
        // Given: a first-attempt backoff of ten seconds
        let policy = RetryPolicy::default();
        // When: the runner asks to be retried no sooner than five minutes
        let due = policy.due_at(
            at(),
            AttemptNumber::FIRST,
            Jitter::MIDPOINT,
            Some(TimeDelta::minutes(5)),
        );
        // Then: the longer of the two wins
        assert_eq!(due, at() + TimeDelta::minutes(5));
    }

    #[test]
    fn a_runner_hint_may_never_shorten_the_delay() {
        // Given: a first-attempt backoff of ten seconds
        let policy = RetryPolicy::default();
        // When: the runner asks to be retried after one second
        let due = policy.due_at(
            at(),
            AttemptNumber::FIRST,
            Jitter::MIDPOINT,
            Some(TimeDelta::seconds(1)),
        );
        // Then: the platform backoff still governs
        assert_eq!(due, at() + TimeDelta::seconds(10));
    }

    #[test]
    fn a_jitter_draw_outside_its_range_is_refused() {
        // Given: a draw expressed in basis points
        // When/Then: only the closed zero-to-one range is admitted
        assert!(Jitter::from_basis_points(-1).is_err());
        assert!(Jitter::from_basis_points(10_001).is_err());
    }
}
