use serde::{Deserialize, Serialize};

use crate::error::JobsError;

const LOWEST: u8 = 1;
const HIGHEST: u8 = 5;

/// A priority level, from 1 (lowest) to 5 (highest).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "u8", into = "u8")]
pub struct Priority(u8);

impl Priority {
    pub const LOWEST: Self = Self(LOWEST);
    /// The level a Job requests when its producer names none.
    pub const DEFAULT: Self = Self(3);
    pub const HIGHEST: Self = Self(HIGHEST);

    pub fn new(value: i64) -> Result<Self, JobsError> {
        match u8::try_from(value) {
            Ok(level) if (LOWEST..=HIGHEST).contains(&level) => Ok(Self(level)),
            _ => Err(JobsError::InvalidPriority { value }),
        }
    }

    pub fn get(self) -> u8 {
        self.0
    }

    pub fn get_i16(self) -> i16 {
        i16::from(self.0)
    }

    /// The level once a maximum applies: never above the maximum, never raised by it.
    pub fn capped_by(self, maximum: Self) -> Self {
        self.min(maximum)
    }
}

impl TryFrom<u8> for Priority {
    type Error = JobsError;

    fn try_from(value: u8) -> Result<Self, JobsError> {
        Self::new(i64::from(value))
    }
}

impl From<Priority> for u8 {
    fn from(value: Priority) -> Self {
        value.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_priority_is_a_level_from_one_to_five() {
        // Given: every level the scale admits
        // When/Then: each is accepted and reads back as itself
        for level in 1..=5 {
            assert_eq!(i64::from(Priority::new(level).unwrap().get()), level);
        }
    }

    #[test]
    fn a_level_outside_the_scale_is_refused_with_the_value_it_carried() {
        // Given: levels below, above and far outside the scale
        for value in [0, 6, -1, 300, i64::MAX] {
            // When/Then: each is refused under one stable code naming the value
            assert_eq!(
                Priority::new(value),
                Err(JobsError::InvalidPriority { value })
            );
        }
    }

    #[test]
    fn a_job_created_without_a_priority_requests_three() {
        // Given/When/Then: the default request is the middle of the scale
        assert_eq!(Priority::DEFAULT.get(), 3);
    }

    #[test]
    fn a_maximum_lowers_a_request_above_it_and_never_raises_one_below_it() {
        // Given: a maximum of 3
        let maximum = Priority::new(3).unwrap();
        // When/Then: a request of 5 is lowered to 3, a request of 2 stays 2
        assert_eq!(Priority::HIGHEST.capped_by(maximum), maximum);
        assert_eq!(
            Priority::new(2).unwrap().capped_by(maximum),
            Priority::new(2).unwrap()
        );
    }

    #[test]
    fn the_wire_form_of_a_priority_is_its_number_and_out_of_range_never_parses() {
        // Given: a priority travelling in an event payload
        // When/Then: it round-trips as a number, and a number outside the scale is refused
        assert_eq!(serde_json::to_string(&Priority::HIGHEST).unwrap(), "5");
        assert_eq!(
            serde_json::from_str::<Priority>("4").unwrap(),
            Priority::new(4).unwrap()
        );
        assert!(serde_json::from_str::<Priority>("0").is_err());
        assert!(serde_json::from_str::<Priority>("6").is_err());
    }
}
