use super::*;
use crate::fixtures::plan;

#[test]
fn a_job_shows_the_progression_of_the_attempt_that_is_running() {
    // Given: a job whose second attempt has declared a plan and started a step
    let first = RunBuilder::new(1)
        .started(ts(5))
        .with_plan(plan(&["fetch"]))
        .with_step(0, "fetch", ts(6))
        .failed(ts(20), RunFailureKind::Transient)
        .retry_due(ts(80))
        .build();
    let schedule = first.retry_schedule().unwrap().id();
    let job = JobBuilder::new()
        .with_run(first)
        .with_run(
            RunBuilder::new(2)
                .retry_of_schedule(schedule)
                .started(ts(90))
                .with_step(0, "fetch again", ts(91))
                .build(),
        )
        .build();
    // When: the job's progression is read
    let progression = job.progression().unwrap();
    // Then: it is the live attempt's, never the abandoned one's
    assert_eq!(
        progression.current_step().map(|step| step.label().as_str()),
        Some("fetch again")
    );
}

#[test]
fn a_settled_job_still_shows_where_its_last_attempt_stopped() {
    // Given: a job whose only attempt failed after starting a step
    let job = JobBuilder::new()
        .with_run(
            RunBuilder::new(1)
                .started(ts(5))
                .with_step(0, "fetch", ts(6))
                .failed(ts(20), RunFailureKind::Permanent)
                .build(),
        )
        .with_resolution(
            JobResolution::failed(
                resolution_id(),
                ts(30),
                JobFailureCause::DeclaredByOwner,
                None,
            )
            .unwrap(),
        )
        .build();
    // When: the progression is read
    // Then: the audit reader still sees how far the work got
    assert!(job.progression().is_some());
}

#[test]
fn a_job_that_never_ran_has_no_progression() {
    // Given: a queued job with no attempt yet
    let job = JobBuilder::new().build();
    // When/Then: there is nothing to show
    assert!(job.progression().is_none());
}

#[test]
fn a_retry_attempt_names_the_attempt_it_replaces() {
    // Given: a failed attempt whose scheduled retry was dispatched
    let failed = RunBuilder::new(1)
        .started(ts(5))
        .failed(ts(20), RunFailureKind::Transient)
        .retry_due(ts(80))
        .build();
    let failed_id = failed.id();
    let schedule = failed.retry_schedule().unwrap().id();
    let job = JobBuilder::new()
        .with_run(failed)
        .with_run(RunBuilder::new(2).retry_of_schedule(schedule).build())
        .build();
    // When: the retry attempt is asked what it retries
    let retried = job.automatic_retry_of_run_id(job.latest_run().unwrap());
    // Then: the failed attempt is named, so no reader has to join on the schedule itself
    assert_eq!(retried, Some(failed_id));
}

#[test]
fn a_first_attempt_replaces_nothing() {
    // Given: a job on its first attempt
    let job = JobBuilder::new()
        .with_run(RunBuilder::new(1).build())
        .build();
    // When/Then: there is no earlier attempt to name
    assert_eq!(
        job.automatic_retry_of_run_id(job.latest_run().unwrap()),
        None
    );
}
