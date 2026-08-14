use super::*;
use crate::domain::config::RunnerConfig;
use crate::domain::job::resolution::{JobFailureCause, JobResolution};
use crate::domain::run::failure::RunFailureKind;
use crate::fixtures::{
    JobBuilder, RunBuilder, job_id, manual_retry_id, resolution_id, run_id, source_entity_id, ts,
    user,
};
use serde_json::json;

mod redelivery;
mod successor;

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
    let plan = plan(manual_retry(&predecessor, ParentContext::Root, None, command).unwrap());
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
    let result = manual_retry(
        &predecessor,
        ParentContext::Root,
        Some(&live),
        command(resolution),
    );
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
    let outcome = manual_retry(
        &predecessor,
        ParentContext::Root,
        Some(&settled),
        command(resolution),
    );
    // Then: the successor is queued — nothing live competes for the source
    assert!(matches!(outcome.unwrap(), ManualRetryOutcome::Started(_)));
}

#[test]
fn a_job_holding_an_unrelated_source_never_blocks_the_retry_silently() {
    // Given: a live job that holds some other source reference entirely
    let resolution = resolution_id();
    let predecessor = failed_predecessor(resolution).build();
    let unrelated = JobBuilder::new()
        .with_source(source_entity_id())
        .with_run(RunBuilder::new(1).started(ts(5)).build())
        .build();
    // When: it is offered as the holder of the predecessor's source
    let result = manual_retry(
        &predecessor,
        ParentContext::Root,
        Some(&unrelated),
        command(resolution),
    );
    // Then: the mismatch fails loud rather than refusing a legitimate intervention
    assert_eq!(
        result,
        Err(JobsError::CorruptState {
            reason_code: "active_job_is_not_for_the_command_source"
        })
    );
}

#[test]
fn a_stale_failed_resolution_is_rejected() {
    // Given: a failed job whose current resolution differs from the one pinned
    let resolution = resolution_id();
    let predecessor = failed_predecessor(resolution).build();
    // When: the administrator acts on a resolution that is no longer current
    let result = manual_retry(
        &predecessor,
        ParentContext::Root,
        None,
        command(resolution_id()),
    );
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
    let result = manual_retry(&predecessor, ParentContext::Root, None, command(resolution));
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
    let result = manual_retry(&predecessor, ParentContext::Root, None, command(resolution));
    // Then: the audit record spawns no new work
    assert_eq!(result, Err(JobsError::JobDeleted));
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
    let result = manual_retry(
        &predecessor,
        ParentContext::Loaded(&parent),
        None,
        command(resolution),
    );
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
    let predecessor = failed_predecessor(resolution).with_parent(job_id()).build();
    // When: the retry is judged with no parent supplied
    let result = manual_retry(&predecessor, ParentContext::Root, None, command(resolution));
    // Then: the parent is a precondition of the judgement, never assumed away
    assert_eq!(
        result,
        Err(JobsError::CorruptState {
            reason_code: "parent_job_not_loaded"
        })
    );
}
