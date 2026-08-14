use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::domain::keys::StepLabel;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct StepIndex(u32);

impl StepIndex {
    pub const FIRST: Self = Self(0);

    pub fn new(value: u32) -> Self {
        Self(value)
    }

    pub fn get(&self) -> u32 {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Step {
    index: StepIndex,
    label: StepLabel,
    started_at: DateTime<Utc>,
}

impl Step {
    pub fn new(index: StepIndex, label: StepLabel, started_at: DateTime<Utc>) -> Self {
        Self {
            index,
            label,
            started_at,
        }
    }

    pub fn index(&self) -> StepIndex {
        self.index
    }

    pub fn label(&self) -> &StepLabel {
        &self.label
    }

    pub fn started_at(&self) -> DateTime<Utc> {
        self.started_at
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_step_records_where_the_runner_is_not_how_far_along_it_is() {
        // Given: a runner starting its third declared step
        let step = Step::new(
            StepIndex::new(2),
            StepLabel::new("Drafting the answer").unwrap(),
            DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
        );
        // When/Then: the step carries a position and a label, never a percentage
        assert_eq!(step.index().get(), 2);
        assert_eq!(step.label().as_str(), "Drafting the answer");
    }
}
