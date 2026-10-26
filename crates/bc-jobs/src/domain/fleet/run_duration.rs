use chrono::TimeDelta;
use serde::{Deserialize, Serialize};

use crate::error::JobsError;

const LONGEST_SECONDS: i64 = 366 * 24 * 60 * 60;

/// How long a started Run of a RunnerType may execute before it fails as
/// transient. Positive, and at most a year: past that a duration stops being a
/// backstop and starts being an absence of one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "i64", into = "i64")]
pub struct MaxRunDuration(i64);

impl MaxRunDuration {
    /// The service default when nothing configures another one: 72 hours.
    pub const SERVICE_DEFAULT: Self = Self(72 * 60 * 60);

    pub fn from_seconds(seconds: i64) -> Result<Self, JobsError> {
        if seconds <= 0 {
            return Err(JobsError::InvalidDuration {
                field: "max_run_duration",
            });
        }
        if seconds > LONGEST_SECONDS {
            return Err(JobsError::OutOfRange {
                field: "max_run_duration",
                value: seconds,
            });
        }
        Ok(Self(seconds))
    }

    pub fn from_delta(delta: TimeDelta) -> Result<Self, JobsError> {
        Self::from_seconds(delta.num_seconds())
    }

    pub fn seconds(self) -> i64 {
        self.0
    }

    pub fn as_delta(self) -> TimeDelta {
        TimeDelta::seconds(self.0)
    }
}

impl TryFrom<i64> for MaxRunDuration {
    type Error = JobsError;

    fn try_from(value: i64) -> Result<Self, JobsError> {
        Self::from_seconds(value)
    }
}

impl From<MaxRunDuration> for i64 {
    fn from(value: MaxRunDuration) -> Self {
        value.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_maximum_run_duration_is_a_positive_number_of_seconds() {
        // Given: 96 hours
        let duration = MaxRunDuration::from_seconds(96 * 3600).unwrap();
        // When/Then: it reads back as itself, in seconds and as a delta
        assert_eq!(duration.seconds(), 345_600);
        assert_eq!(duration.as_delta(), TimeDelta::hours(96));
    }

    #[test]
    fn a_zero_or_negative_duration_is_refused() {
        // Given: durations that would fail every run the moment it starts
        for seconds in [0, -1] {
            // When/Then: each is refused naming the field
            assert_eq!(
                MaxRunDuration::from_seconds(seconds),
                Err(JobsError::InvalidDuration {
                    field: "max_run_duration"
                })
            );
        }
    }

    #[test]
    fn a_duration_beyond_a_year_is_refused_with_its_value() {
        // Given: a duration no backstop would ever reach
        let seconds = LONGEST_SECONDS + 1;
        // When/Then: it is refused with the value carried
        assert_eq!(
            MaxRunDuration::from_seconds(seconds),
            Err(JobsError::OutOfRange {
                field: "max_run_duration",
                value: seconds
            })
        );
    }
}
