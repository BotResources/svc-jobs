use super::*;
use crate::domain::attempts::MaxAttempts;
use crate::domain::config::RunnerConfig;
use crate::domain::job::resolution::{JobFailureCause, JobResolution};
use crate::domain::run::failure::RunFailureKind;
use crate::fixtures::{
    JobBuilder, RunBuilder, job_id, manual_retry_id, resolution_id, run_id, source_entity_id, ts,
    user,
};
use serde_json::json;

fn config() -> RunnerConfig {
    RunnerConfig::new(json!({ "prompt": "summarise", "depth": 3 })).unwrap()
}

fn failed_predecessor(resolution: ResolutionId) -> JobBuilder {
    JobBuilder::new()
        .with_max_attempts(2)
        .with_config(config())
        .with_source(source_entity_id())
        .with_run(
            RunBuilder::new(1)
                .started(ts(5))
                .failed(ts(20), RunFailureKind::Permanent)
                .build(),
        )
        .with_resolution(
            JobResolution::failed(resolution, ts(30), JobFailureCause::DeclaredByOwner, None)
                .unwrap(),
        )
}

fn command(failed_resolution_id: ResolutionId) -> ManualRetryJob {
    ManualRetryJob {
        manual_retry_id: manual_retry_id(),
        failed_resolution_id,
        successor_job_id: job_id(),
        first_run_id: run_id(),
        requested_by: user(),
    }
}

fn plan(outcome: ManualRetryOutcome) -> ManualRetryPlan {
    match outcome {
        ManualRetryOutcome::Started(plan) => *plan,
        ManualRetryOutcome::AlreadyStarted => panic!("expected the retry to start"),
    }
}

#[test]
fn a_manual_retry_records_the_intervention_on_the_failed_predecessor() {
    // Given: a failed job an administrator wants to retry
    let resolution = resolution_id();
    let predecessor = failed_predecessor(resolution).build();
    let command = command(resolution);
    let successor_job_id = command.successor_job_id;
    // When: the intervention is recorded
    let plan = plan(manual_retry(&predecessor, None, None, command).unwrap());
    // Then: the predecessor records the human act, pinning the resolution retried
    match plan.predecessor.events.as_slice() {
        [JobEvent::ManualRetryStarted(fact)] => {
            assert_eq!(fact.job_id, predecessor.id());
            assert_eq!(fact.failed_resolution_id, resolution);
            assert_eq!(fact.successor_job_id, successor_job_id);
        }
        other => panic!("expected a ManualRetryStarted fact, got {other:?}"),
    }
}

#[test]
fn the_successor_copies_the_declaration_and_is_dispatched_at_once() {
    // Given: a failed job carrying a source, a budget and a runner type
    let resolution = resolution_id();
    let predecessor = failed_predecessor(resolution).build();
    // When: it is manually retried
    let plan = plan(manual_retry(&predecessor, None, None, command(resolution)).unwrap());
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
    let plan = plan(manual_retry(&predecessor, None, None, command(resolution)).unwrap());
    // Then: the successor starts on a fresh budget, its first run dispatched
    assert!(matches!(
        plan.successor.events.last(),
        Some(JobEvent::RunDispatched(_))
    ));
}

#[test]
fn a_redelivered_intervention_is_absorbed() {
    // Given: a predecessor that already records this exact intervention
    let resolution = resolution_id();
    let command = command(resolution);
    let predecessor = failed_predecessor(resolution)
        .with_manual_retry(crate::domain::job::parts::ManualRetryRecord::new(
            command.manual_retry_id,
            resolution,
            command.successor_job_id,
            user(),
            ts(60),
            false,
        ))
        .build();
    // When: the same command is delivered again
    let outcome = manual_retry(&predecessor, None, None, command).unwrap();
    // Then: no second successor is created
    assert_eq!(outcome, ManualRetryOutcome::AlreadyStarted);
}

#[test]
fn reusing_an_intervention_id_for_another_successor_is_rejected() {
    // Given: a predecessor whose intervention already points at a successor
    let resolution = resolution_id();
    let first = command(resolution);
    let predecessor = failed_predecessor(resolution)
        .with_manual_retry(crate::domain::job::parts::ManualRetryRecord::new(
            first.manual_retry_id,
            resolution,
            first.successor_job_id,
            user(),
            ts(60),
            false,
        ))
        .build();
    // When: the same intervention id names a different successor
    let conflicting = ManualRetryJob {
        successor_job_id: job_id(),
        ..first.clone()
    };
    let result = manual_retry(&predecessor, None, None, conflicting);
    // Then: the conflict is rejected, naming the successor already recorded
    assert_eq!(
        result,
        Err(JobsError::ManualRetryConflict {
            successor_job_id: first.successor_job_id.as_uuid()
        })
    );
}

#[test]
fn a_retry_whose_source_a_live_job_has_taken_over_is_rejected() {
    // Given: a failed job whose source reference a newer job already occupies
    let resolution = resolution_id();
    let source = source_entity_id();
    let predecessor = failed_predecessor(resolution).with_source(source).build();
    let live = JobBuilder::new()
        .with_source(source)
        .with_run(RunBuilder::new(1).started(ts(5)).build())
        .build();
    // When: an administrator retries the old one, which would copy that same source
    let result = manual_retry(&predecessor, None, Some(&live), command(resolution));
    // Then: it is refused — one source reference never carries two live jobs
    assert_eq!(
        result,
        Err(JobsError::SourceAlreadyActive {
            active_job_id: live.id().as_uuid()
        })
    );
}

#[test]
fn a_retry_is_admitted_once_the_job_holding_its_source_has_settled() {
    // Given: a failed job whose source is held only by another settled job
    let resolution = resolution_id();
    let source = source_entity_id();
    let predecessor = failed_predecessor(resolution).with_source(source).build();
    let settled = JobBuilder::new()
        .with_source(source)
        .with_resolution(JobResolution::completed(resolution_id(), ts(40)))
        .build();
    // When: the administrator retries it
    let outcome = manual_retry(&predecessor, None, Some(&settled), command(resolution));
    // Then: the successor is queued — nothing live competes for the source
    assert!(matches!(outcome.unwrap(), ManualRetryOutcome::Started(_)));
}

#[test]
fn a_stale_failed_resolution_is_rejected() {
    // Given: a failed job whose current resolution differs from the one pinned
    let resolution = resolution_id();
    let predecessor = failed_predecessor(resolution).build();
    // When: the administrator acts on a resolution that is no longer current
    let result = manual_retry(&predecessor, None, None, command(resolution_id()));
    // Then: it is rejected, naming the resolution that is current
    assert_eq!(
        result,
        Err(JobsError::StaleFailedResolution {
            current_resolution_id: resolution.as_uuid()
        })
    );
}

#[test]
fn a_job_that_did_not_fail_may_not_be_manually_retried() {
    // Given: a job that completed
    let resolution = resolution_id();
    let predecessor = JobBuilder::new()
        .with_resolution(JobResolution::completed(resolution, ts(30)))
        .build();
    // When: a manual retry is attempted
    let result = manual_retry(&predecessor, None, None, command(resolution));
    // Then: only failed jobs are retried
    assert_eq!(
        result,
        Err(JobsError::JobNotFailed {
            status: "COMPLETED"
        })
    );
}

#[test]
fn a_deleted_job_may_not_be_manually_retried() {
    // Given: a failed job that was soft-deleted
    let resolution = resolution_id();
    let predecessor = failed_predecessor(resolution).deleted(ts(90)).build();
    // When: a manual retry is attempted
    let result = manual_retry(&predecessor, None, None, command(resolution));
    // Then: the audit record spawns no new work
    assert_eq!(result, Err(JobsError::JobDeleted));
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
    let plan = plan(manual_retry(&predecessor, Some(&parent), None, command(resolution)).unwrap());
    // Then: the successor is queued under the same parent and the same owner
    match plan.successor.events.first() {
        Some(JobEvent::JobQueued(queued)) => {
            assert_eq!(queued.parent_job_id, Some(parent.id()));
            assert_eq!(queued.owner, predecessor.owner().clone());
        }
        other => panic!("expected a queued successor, got {other:?}"),
    }
}

#[test]
fn no_successor_is_ever_born_under_a_parent_that_has_finished() {
    // Given: a failed child whose parent failed after it
    let resolution = resolution_id();
    let parent = failed_predecessor(resolution_id()).build();
    let predecessor = failed_predecessor(resolution)
        .with_parent(parent.id())
        .build();
    // When: an administrator retries the child
    let result = manual_retry(&predecessor, Some(&parent), None, command(resolution));
    // Then: it is refused exactly as a fresh creation would be — nobody could resolve it
    assert_eq!(
        result,
        Err(JobsError::ParentJobTerminal {
            parent_job_id: parent.id().as_uuid()
        })
    );
}

#[test]
fn retrying_a_child_without_its_parent_loaded_is_refused_rather_than_assumed() {
    // Given: a failed child job
    let resolution = resolution_id();
    let parent_job_id = job_id();
    let predecessor = failed_predecessor(resolution)
        .with_parent(parent_job_id)
        .build();
    // When: the retry is judged with no parent supplied
    let result = manual_retry(&predecessor, None, None, command(resolution));
    // Then: the parent is a precondition, never assumed alive
    assert_eq!(
        result,
        Err(JobsError::ParentJobUnknown {
            parent_job_id: parent_job_id.as_uuid()
        })
    );
}
