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

fn positive(field: &'static str, duration: TimeDelta) -> Result<TimeDelta, JobsError> {
    if duration > TimeDelta::zero() {
        Ok(duration)
    } else {
        Err(JobsError::InvalidDuration { field })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServiceLimits {
    max_attempts_ceiling: MaxAttempts,
    default_max_attempts: MaxAttempts,
    max_run_duration: TimeDelta,
    inactivity_timeout: TimeDelta,
}

impl Default for ServiceLimits {
    fn default() -> Self {
        Self {
            max_attempts_ceiling: MaxAttempts::new(10).expect("ten is a positive attempt budget"),
            default_max_attempts: MaxAttempts::new(3).expect("three is a positive attempt budget"),
            max_run_duration: TimeDelta::hours(72),
            inactivity_timeout: TimeDelta::hours(24),
        }
    }
}

impl ServiceLimits {
    pub fn new(
        max_attempts_ceiling: MaxAttempts,
        default_max_attempts: MaxAttempts,
        max_run_duration: TimeDelta,
        inactivity_timeout: TimeDelta,
    ) -> Result<Self, JobsError> {
        default_max_attempts.guard_under_ceiling(max_attempts_ceiling)?;
        Ok(Self {
            max_attempts_ceiling,
            default_max_attempts,
            max_run_duration: positive("max_run_duration", max_run_duration)?,
            inactivity_timeout: positive("inactivity_timeout", inactivity_timeout)?,
        })
    }

    pub fn max_attempts_ceiling(&self) -> MaxAttempts {
        self.max_attempts_ceiling
    }

    pub fn default_max_attempts(&self) -> MaxAttempts {
        self.default_max_attempts
    }

    pub fn max_run_duration(&self) -> TimeDelta {
        self.max_run_duration
    }

    pub fn inactivity_timeout(&self) -> TimeDelta {
        self.inactivity_timeout
    }

    pub fn budget(&self, declared: Option<MaxAttempts>) -> Result<MaxAttempts, JobsError> {
        declared
            .unwrap_or(self.default_max_attempts)
            .guard_under_ceiling(self.max_attempts_ceiling)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    base_delay: TimeDelta,
    factor: u32,
    max_delay: TimeDelta,
    jitter_span_basis_points: i64,
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
    pub fn new(
        base_delay: TimeDelta,
        factor: u32,
        max_delay: TimeDelta,
        jitter_span_basis_points: i64,
    ) -> Result<Self, JobsError> {
        let base_delay = positive("base_delay", base_delay)?;
        let max_delay = positive("max_delay", max_delay)?;
        if factor == 0 {
            return Err(JobsError::InvalidRetryFactor);
        }
        if max_delay < base_delay {
            return Err(JobsError::InvalidDuration { field: "max_delay" });
        }
        Jitter::from_basis_points(jitter_span_basis_points)?;
        Ok(Self {
            base_delay,
            factor,
            max_delay,
            jitter_span_basis_points,
        })
    }

    pub fn base_delay(&self) -> TimeDelta {
        self.base_delay
    }

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
mod tests;
