use super::*;
use crate::fixtures::{JobBuilder, RunBuilder, instance, ts};

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
