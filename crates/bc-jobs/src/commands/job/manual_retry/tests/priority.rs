use super::*;

fn level(value: i64) -> Priority {
    Priority::new(value).unwrap()
}

fn queued_successor(plan: &ManualRetryPlan) -> &JobQueued {
    match plan.successor.events.first() {
        Some(JobEvent::JobQueued(queued)) => queued,
        other => panic!("expected a queued successor, got {other:?}"),
    }
}

#[test]
fn a_successor_copies_the_requested_priority_and_is_capped_again() {
    let resolution = resolution_id();
    let predecessor = failed_predecessor(resolution)
        .with_priority(level(5), level(5), Some(level(5)))
        .build();
    let command = ManualRetryJob {
        applicable_maximum: level(2),
        ..command(resolution)
    };
    let plan = plan(manual_retry(&predecessor, ParentContext::Root, None, command).unwrap());
    let queued = queued_successor(&plan);
    assert_eq!(queued.requested_priority, level(5));
    assert_eq!(queued.effective_priority, level(2));
}

#[test]
fn a_successor_under_a_parent_takes_the_parents_effective_priority() {
    let resolution = resolution_id();
    let parent = JobBuilder::new()
        .with_priority(level(4), level(4), None)
        .with_run(RunBuilder::new(1).started(ts(5)).build())
        .build();
    let predecessor = failed_predecessor(resolution)
        .with_parent(parent.id())
        .with_priority(level(1), level(1), None)
        .build();
    let plan = plan(
        manual_retry(
            &predecessor,
            ParentContext::Loaded(&parent),
            None,
            command(resolution),
        )
        .unwrap(),
    );
    assert_eq!(queued_successor(&plan).effective_priority, level(4));
    assert_eq!(queued_successor(&plan).requested_priority, level(4));
}

#[test]
fn a_successor_of_a_claim_type_waits_to_be_claimed() {
    let resolution = resolution_id();
    let predecessor = failed_predecessor(resolution).build();
    let command = ManualRetryJob {
        dispatch: SuccessorDispatch::Wait,
        ..command(resolution)
    };
    let plan = plan(manual_retry(&predecessor, ParentContext::Root, None, command).unwrap());
    assert!(matches!(
        plan.successor.events.as_slice(),
        [JobEvent::JobQueued(_)]
    ));
    match plan.predecessor.events.as_slice() {
        [JobEvent::ManualRetryStarted(fact)] => assert_eq!(fact.run_id, None),
        other => panic!("expected the intervention, got {other:?}"),
    }
}
