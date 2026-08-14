use serde::{Deserialize, Serialize};

use crate::error::JobsError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "u32", into = "u32")]
pub struct AttemptNumber(u32);

impl AttemptNumber {
    pub const FIRST: Self = Self(1);

    pub fn new(value: u32) -> Result<Self, JobsError> {
        if value == 0 {
            Err(JobsError::CorruptState {
                reason_code: "attempt_number_below_one",
            })
        } else {
            Ok(Self(value))
        }
    }

    pub fn get(&self) -> u32 {
        self.0
    }

    pub fn next(&self) -> Self {
        Self(self.0.saturating_add(1))
    }

    pub fn is_first(&self) -> bool {
        self.0 == 1
    }
}

impl From<AttemptNumber> for u32 {
    fn from(value: AttemptNumber) -> Self {
        value.0
    }
}

impl TryFrom<u32> for AttemptNumber {
    type Error = JobsError;

    fn try_from(value: u32) -> Result<Self, JobsError> {
        Self::new(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "u32", into = "u32")]
pub struct MaxAttempts(u32);

impl MaxAttempts {
    pub fn new(value: u32) -> Result<Self, JobsError> {
        if value == 0 {
            Err(JobsError::CorruptState {
                reason_code: "max_attempts_below_one",
            })
        } else {
            Ok(Self(value))
        }
    }

    pub fn under_ceiling(value: u32, ceiling: u32) -> Result<Self, JobsError> {
        let requested = Self::new(value)?;
        if requested.0 > ceiling {
            Err(JobsError::MaxAttemptsAboveCeiling {
                requested: value,
                ceiling,
            })
        } else {
            Ok(requested)
        }
    }

    pub fn get(&self) -> u32 {
        self.0
    }

    pub fn allows(&self, attempt: AttemptNumber) -> bool {
        attempt.get() <= self.0
    }
}

impl From<MaxAttempts> for u32 {
    fn from(value: MaxAttempts) -> Self {
        value.0
    }
}

impl TryFrom<u32> for MaxAttempts {
    type Error = JobsError;

    fn try_from(value: u32) -> Result<Self, JobsError> {
        Self::new(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_caller_may_lower_the_attempt_budget() {
        // Given: a service ceiling of ten attempts
        // When: a producer asks for three
        let budget = MaxAttempts::under_ceiling(3, 10).unwrap();
        // Then: the lower figure is honoured
        assert_eq!(budget.get(), 3);
    }

    #[test]
    fn a_caller_may_never_raise_the_attempt_budget_above_the_ceiling() {
        // Given: a service ceiling of ten attempts
        // When: a producer asks for fifty
        let result = MaxAttempts::under_ceiling(50, 10);
        // Then: the request is refused with both figures in the params
        assert_eq!(
            result,
            Err(JobsError::MaxAttemptsAboveCeiling {
                requested: 50,
                ceiling: 10
            })
        );
    }

    #[test]
    fn a_budget_admits_exactly_its_own_attempt_and_no_further() {
        // Given: a budget of two attempts
        let budget = MaxAttempts::new(2).unwrap();
        // When/Then: the second attempt is allowed, the third is not
        assert!(budget.allows(AttemptNumber::new(2).unwrap()));
        assert!(!budget.allows(AttemptNumber::new(3).unwrap()));
    }

    #[test]
    fn attempt_numbering_starts_at_one() {
        // Given: the zero attempt, which no run may ever carry
        // When/Then: it is refused, and the first attempt is one
        assert!(AttemptNumber::new(0).is_err());
        assert_eq!(AttemptNumber::FIRST.get(), 1);
        assert!(AttemptNumber::FIRST.is_first());
    }
}
