use serde::{Deserialize, Serialize};

use crate::error::JobsError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "u32", into = "u32")]
pub struct Capacity(u32);

impl Capacity {
    pub fn new(value: u32) -> Result<Self, JobsError> {
        if value == 0 {
            return Err(JobsError::OutOfRange {
                field: "capacity",
                value: 0,
            });
        }
        Ok(Self(value))
    }

    pub fn get(self) -> u32 {
        self.0
    }

    pub fn is_saturated_by(self, current_runs: usize) -> bool {
        current_runs >= self.0 as usize
    }
}

impl TryFrom<u32> for Capacity {
    type Error = JobsError;

    fn try_from(value: u32) -> Result<Self, JobsError> {
        Self::new(value)
    }
}

impl From<Capacity> for u32 {
    fn from(value: Capacity) -> Self {
        value.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_instance_declaring_no_capacity_at_all_is_refused() {
        // Given: a runner announcing it can carry zero runs
        // When: that declaration enters the domain
        // Then: it is refused — a live instance carries at least one run, or it is not live
        assert_eq!(
            Capacity::new(0),
            Err(JobsError::OutOfRange {
                field: "capacity",
                value: 0
            })
        );
        assert_eq!(Capacity::new(1).unwrap().get(), 1);
    }

    #[test]
    fn an_instance_is_saturated_only_once_it_holds_as_many_runs_as_it_declared() {
        // Given: an instance that declared room for three runs
        let capacity = Capacity::new(3).unwrap();
        // When/Then: it is free below three and saturated from three on
        assert!(!capacity.is_saturated_by(0));
        assert!(!capacity.is_saturated_by(2));
        assert!(capacity.is_saturated_by(3));
        assert!(capacity.is_saturated_by(4));
    }

    #[test]
    fn the_wire_form_of_a_capacity_is_its_number_and_zero_never_parses() {
        // Given: a declared capacity travelling in an event payload
        // When/Then: it round-trips as a plain number, and zero is refused on the way in
        assert_eq!(
            serde_json::to_string(&Capacity::new(4).unwrap()).unwrap(),
            "4"
        );
        assert_eq!(
            serde_json::from_str::<Capacity>("4").unwrap(),
            Capacity::new(4).unwrap()
        );
        assert!(serde_json::from_str::<Capacity>("0").is_err());
        assert!(serde_json::from_str::<Capacity>("-1").is_err());
    }
}
