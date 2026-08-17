use super::*;
use crate::domain::attempts::MaxAttempts;

#[test]
fn the_successor_copies_the_declaration_and_is_dispatched_at_once() {
    // Given: a failed job carrying a source, a budget and a runner type
    let resolution = resolution_id();
    let predecessor = failed_predecessor(resolution).build();
    // When: it is manually retried
    let plan =
        plan(manual_retry(&predecessor, ParentContext::Root, None, command(resolution)).unwrap());
    // Then: the successor copies the declaration and its first run goes out
    match plan.successor.events.as_slice() {
        [
            JobEvent::JobQueued(queued),
            JobEvent::RunDispatched(dispatched),
        ] => {
            assert_eq!(queued.predecessor_job_id, Some(predecessor.id()));
            assert_eq!(queued.runner_type, predecessor.runner_type().clone());
            assert_eq!(queued.max_attempts, Some(MaxAttempts::new(2).unwrap()));
            assert_eq!(queued.source, predecessor.source());
            assert_eq!(queued.config, Some(config()));
            assert_eq!(queued.owner, predecessor.owner().clone());
            assert_eq!(dispatched.attempt_number, AttemptNumber::FIRST);
            assert_eq!(dispatched.origin, RunOrigin::ManualRetry);
        }
        other => panic!("expected a queued successor and its run, got {other:?}"),
    }
}

#[test]
fn the_successor_is_dispatched_even_when_the_predecessor_exhausted_its_budget() {
    // Given: a job that failed after spending its single attempt
    let resolution = resolution_id();
    let spent = RunBuilder::new(1)
        .started(ts(5))
        .failed(ts(20), RunFailureKind::Transient)
        .build();
    let spent_run_id = spent.id();
    let predecessor = JobBuilder::new()
        .with_max_attempts(1)
        .with_run(spent)
        .with_resolution(
            JobResolution::failed(
                resolution,
                ts(30),
                JobFailureCause::TerminalRunFailure,
                Some(spent_run_id),
            )
            .unwrap(),
        )
        .build();
    // When: an administrator retries it manually
    let plan =
        plan(manual_retry(&predecessor, ParentContext::Root, None, command(resolution)).unwrap());
    // Then: the successor starts on a fresh budget, its first run dispatched
    assert!(matches!(
        plan.successor.events.last(),
        Some(JobEvent::RunDispatched(_))
    ));
}

#[test]
fn a_failed_child_is_retried_under_the_parent_that_still_owns_it() {
    // Given: a failed child job whose parent is still executing
    let resolution = resolution_id();
    let parent = JobBuilder::new()
        .with_run(RunBuilder::new(1).started(ts(5)).build())
        .build();
    let predecessor = failed_predecessor(resolution)
        .with_parent(parent.id())
        .build();
    // When: an administrator retries it
    let plan = plan(
        manual_retry(
            &predecessor,
            ParentContext::Loaded(&parent),
            None,
            command(resolution),
        )
        .unwrap(),
    );
    // Then: the successor is queued under the same parent and the same owner
    match plan.successor.events.first() {
        Some(JobEvent::JobQueued(queued)) => {
            assert_eq!(queued.parent_job_id, Some(parent.id()));
            assert_eq!(queued.owner, predecessor.owner().clone());
        }
        other => panic!("expected a queued successor, got {other:?}"),
    }
}
