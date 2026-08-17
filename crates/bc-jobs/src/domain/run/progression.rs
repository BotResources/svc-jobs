use crate::domain::run::plan::RunPlan;
use crate::domain::run::step::Step;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunProgression<'a> {
    plan: Option<&'a RunPlan>,
    current_step: Option<&'a Step>,
}

impl<'a> RunProgression<'a> {
    pub fn of(plan: Option<&'a RunPlan>, current_step: Option<&'a Step>) -> Option<Self> {
        (plan.is_some() || current_step.is_some()).then_some(Self { plan, current_step })
    }

    pub fn plan(&self) -> Option<&'a RunPlan> {
        self.plan
    }

    pub fn current_step(&self) -> Option<&'a Step> {
        self.current_step
    }
}

#[cfg(test)]
mod tests {
    use crate::fixtures::{RunBuilder, plan, ts};

    #[test]
    fn a_run_that_declared_nothing_has_no_progression_to_show() {
        // Given: a dispatched run whose runner has neither declared a plan nor started a step
        let run = RunBuilder::new(1).started(ts(5)).build();
        // When/Then: there is nothing to show, rather than an empty shell
        assert_eq!(run.progression(), None);
    }

    #[test]
    fn a_run_shows_its_latest_plan_and_the_step_it_is_on() {
        // Given: a run whose runner declared a plan and started its second step
        let run = RunBuilder::new(1)
            .started(ts(5))
            .with_plan(plan(&["fetch", "summarise"]))
            .with_step(0, "fetch", ts(6))
            .with_step(1, "summarise", ts(9))
            .build();
        // When: the progression is read
        let progression = run.progression().unwrap();
        // Then: the current step is the last one started, alongside the declared plan
        assert_eq!(progression.plan().map(|plan| plan.items().len()), Some(2));
        assert_eq!(
            progression.current_step().map(|step| step.index().get()),
            Some(1)
        );
    }

    #[test]
    fn a_run_stepping_without_a_declared_plan_still_shows_where_it_is() {
        // Given: a runner that reports steps without ever declaring a plan
        let run = RunBuilder::new(1)
            .started(ts(5))
            .with_step(0, "fetch", ts(6))
            .build();
        // When: the progression is read
        let progression = run.progression().unwrap();
        // Then: the step is shown with no plan to measure it against
        assert!(progression.plan().is_none());
        assert!(progression.current_step().is_some());
    }
}
