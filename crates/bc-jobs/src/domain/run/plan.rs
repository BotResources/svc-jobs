use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::domain::ids::PlanDeclarationId;
use crate::domain::keys::StepLabel;
use crate::domain::run::step::StepIndex;
use crate::error::JobsError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "u32", into = "u32")]
pub struct DeclarationNumber(u32);

impl DeclarationNumber {
    pub const FIRST: Self = Self(1);

    pub fn new(value: u32) -> Result<Self, JobsError> {
        if value == 0 {
            Err(JobsError::CorruptState {
                reason_code: "declaration_number_below_one",
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
}

impl From<DeclarationNumber> for u32 {
    fn from(value: DeclarationNumber) -> Self {
        value.0
    }
}

impl TryFrom<u32> for DeclarationNumber {
    type Error = JobsError;

    fn try_from(value: u32) -> Result<Self, JobsError> {
        Self::new(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunPlanItem {
    index: StepIndex,
    label: StepLabel,
}

impl RunPlanItem {
    pub fn new(index: StepIndex, label: StepLabel) -> Self {
        Self { index, label }
    }

    pub fn index(&self) -> StepIndex {
        self.index
    }

    pub fn label(&self) -> &StepLabel {
        &self.label
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunPlan {
    declaration_id: PlanDeclarationId,
    declaration_number: DeclarationNumber,
    declared_at: DateTime<Utc>,
    items: Vec<RunPlanItem>,
}

impl RunPlan {
    pub fn new(
        declaration_id: PlanDeclarationId,
        declaration_number: DeclarationNumber,
        declared_at: DateTime<Utc>,
        items: Vec<RunPlanItem>,
    ) -> Result<Self, JobsError> {
        if items.is_empty() {
            return Err(JobsError::EmptyPlan);
        }
        let mut seen: Vec<StepIndex> = Vec::with_capacity(items.len());
        for item in &items {
            if seen.contains(&item.index()) {
                return Err(JobsError::DuplicatePlanStepIndex {
                    step_index: item.index().get(),
                });
            }
            seen.push(item.index());
        }
        let mut ordered = items;
        ordered.sort_by_key(RunPlanItem::index);
        Ok(Self {
            declaration_id,
            declaration_number,
            declared_at,
            items: ordered,
        })
    }

    pub fn declaration_id(&self) -> PlanDeclarationId {
        self.declaration_id
    }

    pub fn declaration_number(&self) -> DeclarationNumber {
        self.declaration_number
    }

    pub fn declared_at(&self) -> DateTime<Utc> {
        self.declared_at
    }

    pub fn items(&self) -> &[RunPlanItem] {
        &self.items
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn declaration_id() -> PlanDeclarationId {
        PlanDeclarationId::new(Uuid::now_v7()).unwrap()
    }

    fn at() -> DateTime<Utc> {
        DateTime::from_timestamp(1_700_000_000, 0).unwrap()
    }

    fn item(index: u32, label: &str) -> RunPlanItem {
        RunPlanItem::new(StepIndex::new(index), StepLabel::new(label).unwrap())
    }

    #[test]
    fn a_plan_is_ordered_by_step_index_whatever_the_declaration_order() {
        // Given: a runner declaring its steps out of order
        let plan = RunPlan::new(
            declaration_id(),
            DeclarationNumber::FIRST,
            at(),
            vec![item(2, "Answer"), item(0, "Fetch"), item(1, "Read")],
        )
        .unwrap();
        // When/Then: the plan reads as an ordered sequence
        let labels: Vec<&str> = plan.items().iter().map(|i| i.label().as_str()).collect();
        assert_eq!(labels, vec!["Fetch", "Read", "Answer"]);
    }

    #[test]
    fn a_plan_declaring_the_same_index_twice_is_refused() {
        // Given: a plan with two steps at index one
        let result = RunPlan::new(
            declaration_id(),
            DeclarationNumber::FIRST,
            at(),
            vec![item(1, "Read"), item(1, "Re-read")],
        );
        // Then: the declaration is refused — an index addresses one step
        assert_eq!(
            result,
            Err(JobsError::DuplicatePlanStepIndex { step_index: 1 })
        );
    }

    #[test]
    fn an_empty_plan_is_refused() {
        // Given: a declaration with no steps
        let result = RunPlan::new(declaration_id(), DeclarationNumber::FIRST, at(), vec![]);
        // Then: it is refused — declaring nothing is not a plan
        assert_eq!(result, Err(JobsError::EmptyPlan));
    }

    #[test]
    fn declaration_numbering_starts_at_one_and_advances() {
        // Given: the first declaration of a run
        // When/Then: zero is refused and the next declaration supersedes it
        assert!(DeclarationNumber::new(0).is_err());
        assert_eq!(DeclarationNumber::FIRST.next().get(), 2);
    }
}
