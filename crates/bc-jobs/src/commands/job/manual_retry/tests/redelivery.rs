use super::*;
use crate::domain::job::parts::ManualRetryRecord;

#[test]
fn a_redelivered_intervention_is_absorbed() {
    // Given: a predecessor that already records this exact intervention
    let resolution = resolution_id();
    let command = command(resolution);
    let predecessor = failed_predecessor(resolution)
        .with_manual_retry(ManualRetryRecord::new(
            command.manual_retry_id,
            resolution,
            command.successor_job_id,
            user(),
            ts(60),
            false,
        ))
        .build();
    // When: the same command is delivered again
    let outcome = manual_retry(&predecessor, ParentContext::Root, None, command).unwrap();
    // Then: no second successor is created
    assert_eq!(outcome, ManualRetryOutcome::AlreadyStarted);
}

#[test]
fn reusing_an_intervention_id_for_another_successor_is_rejected() {
    // Given: a predecessor whose intervention already points at a successor
    let resolution = resolution_id();
    let first = command(resolution);
    let predecessor = failed_predecessor(resolution)
        .with_manual_retry(ManualRetryRecord::new(
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
    let result = manual_retry(&predecessor, ParentContext::Root, None, conflicting);
    // Then: the conflict is rejected, naming the successor already recorded
    assert_eq!(
        result,
        Err(JobsError::ManualRetryConflict {
            successor_job_id: first.successor_job_id.as_uuid()
        })
    );
}
