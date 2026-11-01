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
    let run = RunBuilder::new(1).build();
    let run_id = run.id();
    let job = JobBuilder::new().with_run(run).build();
    let result = job
        .record_run_started(RunStartedFact {
            run_id,
            instance: instance(),
        })
        .unwrap();
    match result.events.first() {
        Some(JobEvent::RunStarted(fact)) => {
            assert_eq!(fact.run_id, run_id);
            assert_eq!(fact.instance_key.as_str(), "pod-7");
            assert_eq!(fact.start_id, None);
        }
        other => panic!("expected a RunStarted fact, got {other:?}"),
    }
}

#[test]
fn a_redelivered_start_records_nothing_twice() {
    let run = RunBuilder::new(1).started(ts(5)).build();
    let run_id = run.id();
    let job = JobBuilder::new().with_run(run).build();
    let result = job
        .record_run_started(RunStartedFact {
            run_id,
            instance: instance(),
        })
        .unwrap();
    assert!(result.is_empty());
    assert_eq!(
        result.warnings,
        vec![CommandWarning::FactAlreadyRecorded { fact: "RunStarted" }]
    );
}

#[test]
fn a_status_fact_for_a_terminal_job_is_acknowledged_and_discarded() {
    let run = RunBuilder::new(1).cancelled(ts(30)).build();
    let run_id = run.id();
    let job = JobBuilder::new()
        .with_run(run)
        .with_resolution(JobResolution::cancelled(resolution_id(), ts(30)))
        .build();
    let result = job
        .record_run_started(RunStartedFact {
            run_id,
            instance: instance(),
        })
        .unwrap();
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
    let job = JobBuilder::new()
        .with_run(RunBuilder::new(1).build())
        .build();
    let stranger = crate::fixtures::run_id();
    let result = job.record_run_started(RunStartedFact {
        run_id: stranger,
        instance: instance(),
    });
    assert_eq!(
        result,
        Err(JobsError::RunNotFound {
            run_id: stranger.as_uuid()
        })
    );
}

#[test]
fn the_first_plan_declaration_is_number_one() {
    let run = RunBuilder::new(1).started(ts(5)).build();
    let run_id = run.id();
    let job = JobBuilder::new().with_run(run).build();
    let result = job
        .declare_run_plan(RunPlanFact {
            run_id,
            declaration_id: declaration_id(),
            items: items(&["Fetch", "Read", "Answer"]),
            declared_at: ts(6),
        })
        .unwrap();
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
    let run = RunBuilder::new(1)
        .started(ts(5))
        .with_plan(crate::fixtures::plan(&["Fetch", "Read"]))
        .build();
    let run_id = run.id();
    let job = JobBuilder::new().with_run(run).build();
    let result = job
        .declare_run_plan(RunPlanFact {
            run_id,
            declaration_id: declaration_id(),
            items: items(&["Fetch", "Read", "Answer", "Cite"]),
            declared_at: ts(30),
        })
        .unwrap();
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
    let run = RunBuilder::new(1)
        .started(ts(5))
        .with_plan(crate::fixtures::plan(&["Fetch", "Read"]))
        .build();
    let run_id = run.id();
    let job = JobBuilder::new().with_run(run).build();
    let result = job
        .declare_run_plan(RunPlanFact {
            run_id,
            declaration_id: declaration_id(),
            items: items(&["Fetch", "Read"]),
            declared_at: ts(30),
        })
        .unwrap();
    assert!(result.is_empty());
    assert_eq!(
        result.warnings.first().map(CommandWarning::code),
        Some("fact_already_recorded")
    );
}

#[test]
fn starting_a_step_implicitly_closes_the_previous_one() {
    let run = RunBuilder::new(1)
        .started(ts(5))
        .with_step(0, "Fetch", ts(6))
        .build();
    let run_id = run.id();
    let job = JobBuilder::new().with_run(run).build();
    let result = job
        .record_step_started(RunStepFact {
            run_id,
            step_index: StepIndex::new(1),
            label: label("Read"),
            started_at: ts(20),
        })
        .unwrap();
    match result.events.first() {
        Some(JobEvent::RunStepStarted(fact)) => {
            assert_eq!(fact.step_index, StepIndex::new(1));
            assert_eq!(fact.closed_step_index, Some(StepIndex::FIRST));
            assert_eq!(fact.started_at, ts(20));
        }
        other => panic!("expected a RunStepStarted fact, got {other:?}"),
    }
}

#[test]
fn a_step_that_does_not_advance_the_cursor_is_discarded() {
    let run = RunBuilder::new(1)
        .started(ts(5))
        .with_step(2, "Answer", ts(20))
        .build();
    let run_id = run.id();
    let job = JobBuilder::new().with_run(run).build();
    let result = job
        .record_step_started(RunStepFact {
            run_id,
            step_index: StepIndex::new(1),
            label: label("Read"),
            started_at: ts(10),
        })
        .unwrap();
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
    let run = RunBuilder::new(1).build();
    let run_id = run.id();
    let job = JobBuilder::new().with_run(run).build();
    let result = job.record_run_started(RunStartedFact {
        run_id,
        instance: RunnerInstanceReference::new(
            RunnerTypeKey::new("scribe").unwrap(),
            InstanceKey::new("pod-9").unwrap(),
        ),
    });
    assert_eq!(
        result,
        Err(JobsError::RunnerTypeMismatch {
            expected: "analyst".to_owned(),
            claimed: "scribe".to_owned()
        })
    );
}

#[test]
fn a_reserved_run_is_started_only_by_the_instance_that_claimed_it() {
    let run = RunBuilder::new(1).reserved(ts(1)).build();
    let run_id = run.id();
    let job = JobBuilder::new().with_run(run).build();
    let elsewhere = RunnerInstanceReference::new(
        crate::fixtures::runner_type(),
        crate::domain::keys::InstanceKey::new("pod-9").unwrap(),
    );
    let stolen = job.record_run_started(RunStartedFact {
        run_id,
        instance: elsewhere,
    });
    let honoured = job
        .record_run_started(RunStartedFact {
            run_id,
            instance: crate::fixtures::instance(),
        })
        .unwrap();
    assert_eq!(
        stolen,
        Ok(CommandResult::nothing_happened(
            CommandWarning::FactDiscardedReservedElsewhere { fact: "RunStarted" }
        ))
    );
    assert!(matches!(
        honoured.events.as_slice(),
        [JobEvent::RunStarted(_)]
    ));
}

#[test]
fn a_status_stream_start_for_a_run_held_under_a_start_id_is_discarded() {
    let run = RunBuilder::new(1)
        .started_under(instance(), ts(5), crate::fixtures::start_id())
        .build();
    let run_id = run.id();
    let job = JobBuilder::new().with_run(run).build();
    let from_another = RunnerInstanceReference::new(
        crate::fixtures::runner_type(),
        crate::domain::keys::InstanceKey::new("pod-9").unwrap(),
    );
    let result = job
        .record_run_started(RunStartedFact {
            run_id,
            instance: from_another,
        })
        .unwrap();
    assert!(result.is_empty());
}

#[test]
fn a_status_stream_start_for_an_ended_run_is_discarded() {
    let run = RunBuilder::new(1).started(ts(5)).completed(ts(9)).build();
    let run_id = run.id();
    let job = JobBuilder::new().with_run(run).build();
    let result = job
        .record_run_started(RunStartedFact {
            run_id,
            instance: instance(),
        })
        .unwrap();
    assert!(result.is_empty());
    assert_eq!(
        result.warnings,
        vec![CommandWarning::FactDiscardedOnTerminalRun {
            fact: "RunStarted",
            run_status: "COMPLETED"
        }]
    );
}
