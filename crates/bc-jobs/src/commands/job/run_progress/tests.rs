use super::*;
use crate::domain::job::resolution::JobResolution;
use crate::domain::keys::{InstanceKey, RunnerTypeKey};
use crate::fixtures::{JobBuilder, RunBuilder, declaration_id, instance, resolution_id, ts};

fn label(text: &str) -> StepLabel {
    StepLabel::new(text).unwrap()
}

fn items(labels: &[&str]) -> Vec<RunPlanItem> {
    labels
        .iter()
        .enumerate()
        .map(|(index, text)| {
            RunPlanItem::new(StepIndex::new(u32::try_from(index).unwrap()), label(text))
        })
        .collect()
}

#[test]
fn an_instance_claiming_a_pending_run_starts_it() {
    // Given: a dispatched run nobody has claimed
    let run = RunBuilder::new(1).build();
    let run_id = run.id();
    let job = JobBuilder::new().with_run(run).build();
    // When: an instance reports that it began executing
    let result = job
        .record_run_started(RunStartedFact {
            run_id,
            instance: instance(),
        })
        .unwrap();
    // Then: the start is recorded with the instance that claimed it
    match result.events.first() {
        Some(JobEvent::RunStarted(fact)) => {
            assert_eq!(fact.run_id, run_id);
            assert_eq!(fact.instance_key.as_str(), "pod-7");
        }
        other => panic!("expected a RunStarted fact, got {other:?}"),
    }
}

#[test]
fn a_redelivered_start_records_nothing_twice() {
    // Given: a run already started
    let run = RunBuilder::new(1).started(ts(5)).build();
    let run_id = run.id();
    let job = JobBuilder::new().with_run(run).build();
    // When: the same start fact is delivered again
    let result = job
        .record_run_started(RunStartedFact {
            run_id,
            instance: instance(),
        })
        .unwrap();
    // Then: no second fact is written and the message may be acknowledged
    assert!(result.is_empty());
    assert_eq!(
        result.warnings,
        vec![CommandWarning::FactAlreadyRecorded { fact: "RunStarted" }]
    );
}

#[test]
fn a_status_fact_for_a_terminal_job_is_acknowledged_and_discarded() {
    // Given: a job cancelled, its run withdrawn with it
    let run = RunBuilder::new(1).cancelled(ts(30)).build();
    let run_id = run.id();
    let job = JobBuilder::new()
        .with_run(run)
        .with_resolution(JobResolution::cancelled(resolution_id(), ts(30)))
        .build();
    // When: a late start fact arrives from the runner
    let result = job
        .record_run_started(RunStartedFact {
            run_id,
            instance: instance(),
        })
        .unwrap();
    // Then: history is untouched and the caller learns why
    assert!(result.is_empty());
    assert_eq!(
        result.warnings,
        vec![CommandWarning::FactDiscardedOnTerminalJob {
            fact: "RunStarted",
            job_status: "CANCELLED"
        }]
    );
}

#[test]
fn a_fact_naming_a_run_this_job_never_dispatched_is_refused() {
    // Given: a job with one run
    let job = JobBuilder::new()
        .with_run(RunBuilder::new(1).build())
        .build();
    let stranger = crate::fixtures::run_id();
    // When: a fact arrives for a foreign run
    let result = job.record_run_started(RunStartedFact {
        run_id: stranger,
        instance: instance(),
    });
    // Then: it is refused rather than silently attached
    assert_eq!(
        result,
        Err(JobsError::RunNotFound {
            run_id: stranger.as_uuid()
        })
    );
}

#[test]
fn the_first_plan_declaration_is_number_one() {
    // Given: a started run with no plan yet
    let run = RunBuilder::new(1).started(ts(5)).build();
    let run_id = run.id();
    let job = JobBuilder::new().with_run(run).build();
    // When: the runner announces its segmentation
    let result = job
        .declare_run_plan(RunPlanFact {
            run_id,
            declaration_id: declaration_id(),
            items: items(&["Fetch", "Read", "Answer"]),
            declared_at: ts(6),
        })
        .unwrap();
    // Then: declaration one carries every step, so nobody has to re-query
    match result.events.first() {
        Some(JobEvent::RunPlanDeclared(fact)) => {
            assert_eq!(fact.declaration_number, DeclarationNumber::FIRST);
            assert_eq!(fact.items.len(), 3);
        }
        other => panic!("expected a RunPlanDeclared fact, got {other:?}"),
    }
}

#[test]
fn a_revised_plan_supersedes_the_previous_declaration() {
    // Given: a run that already declared a plan
    let run = RunBuilder::new(1)
        .started(ts(5))
        .with_plan(crate::fixtures::plan(&["Fetch", "Read"]))
        .build();
    let run_id = run.id();
    let job = JobBuilder::new().with_run(run).build();
    // When: the runner revises it
    let result = job
        .declare_run_plan(RunPlanFact {
            run_id,
            declaration_id: declaration_id(),
            items: items(&["Fetch", "Read", "Answer", "Cite"]),
            declared_at: ts(30),
        })
        .unwrap();
    // Then: the new declaration is number two — the last one wins
    match result.events.first() {
        Some(JobEvent::RunPlanDeclared(fact)) => {
            assert_eq!(fact.declaration_number.get(), 2);
            assert_eq!(fact.items.len(), 4);
        }
        other => panic!("expected a RunPlanDeclared fact, got {other:?}"),
    }
}

#[test]
fn a_redelivered_identical_plan_declaration_is_absorbed() {
    // Given: a run whose runner already declared exactly this segmentation
    let run = RunBuilder::new(1)
        .started(ts(5))
        .with_plan(crate::fixtures::plan(&["Fetch", "Read"]))
        .build();
    let run_id = run.id();
    let job = JobBuilder::new().with_run(run).build();
    // When: the very same declaration is delivered a second time
    let result = job
        .declare_run_plan(RunPlanFact {
            run_id,
            declaration_id: declaration_id(),
            items: items(&["Fetch", "Read"]),
            declared_at: ts(30),
        })
        .unwrap();
    // Then: no second declaration is opened — an at-least-once transport must not renumber a plan
    assert!(result.is_empty());
    assert_eq!(
        result.warnings.first().map(CommandWarning::code),
        Some("fact_already_recorded")
    );
}

#[test]
fn starting_a_step_implicitly_closes_the_previous_one() {
    // Given: a run currently on step zero
    let run = RunBuilder::new(1)
        .started(ts(5))
        .with_step(0, "Fetch", ts(6))
        .build();
    let run_id = run.id();
    let job = JobBuilder::new().with_run(run).build();
    // When: the runner moves to step one
    let result = job
        .record_step_started(RunStepFact {
            run_id,
            step_index: StepIndex::new(1),
            label: label("Read"),
            started_at: ts(20),
        })
        .unwrap();
    // Then: the fact names both the step opening and the step it closes
    match result.events.first() {
        Some(JobEvent::RunStepStarted(fact)) => {
            assert_eq!(fact.step_index, StepIndex::new(1));
            assert_eq!(fact.closed_step_index, Some(StepIndex::FIRST));
        }
        other => panic!("expected a RunStepStarted fact, got {other:?}"),
    }
}

#[test]
fn a_step_that_does_not_advance_the_cursor_is_discarded() {
    // Given: a run already on step two
    let run = RunBuilder::new(1)
        .started(ts(5))
        .with_step(2, "Answer", ts(20))
        .build();
    let run_id = run.id();
    let job = JobBuilder::new().with_run(run).build();
    // When: a redelivered or out-of-order step one arrives
    let result = job
        .record_step_started(RunStepFact {
            run_id,
            step_index: StepIndex::new(1),
            label: label("Read"),
            started_at: ts(10),
        })
        .unwrap();
    // Then: the progression cursor never moves backwards
    assert!(result.is_empty());
    assert_eq!(
        result.warnings,
        vec![CommandWarning::StepIndexNotAdvancing {
            current: 2,
            submitted: 1
        }]
    );
}

#[test]
fn an_instance_of_another_runner_type_may_not_claim_the_run() {
    // Given: a dispatched run belonging to a job of the analyst runner type
    let run = RunBuilder::new(1).build();
    let run_id = run.id();
    let job = JobBuilder::new().with_run(run).build();
    // When: an instance of a different runner type reports that it began executing
    let result = job.record_run_started(RunStartedFact {
        run_id,
        instance: RunnerInstanceReference::new(
            RunnerTypeKey::new("scribe").unwrap(),
            InstanceKey::new("pod-9").unwrap(),
        ),
    });
    // Then: the claim is refused rather than silently relabelled to the job's own type
    assert_eq!(
        result,
        Err(JobsError::RunnerTypeMismatch {
            expected: "analyst".to_owned(),
            claimed: "scribe".to_owned()
        })
    );
}
