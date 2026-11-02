use super::*;
use crate::commands::job::backstop::ReclaimRun;
use crate::domain::ids::RunId;
use crate::domain::job::resolution::JobResolution;
use crate::domain::policy::Jitter;
use crate::event::job::JobEvent;
use crate::fixtures::{JobBuilder, RunBuilder, resolution_id, schedule_id, ts};

fn reclaim(run_id: RunId) -> ReclaimRun {
    ReclaimRun {
        run_id,
        retry_schedule_id: schedule_id(),
        resolution_id: resolution_id(),
        jitter: Jitter::MIDPOINT,
        at: ts(50),
    }
}

fn terminated(job: &Job, run_id: RunId) -> JobCommandResult {
    job.fail_run_for_terminated_trigger(
        reclaim(run_id),
        &RetryPolicy::default(),
        &ServiceLimits::default(),
    )
    .unwrap()
}

#[test]
fn a_run_that_has_not_started_fails_as_permanent_and_ends_its_job_with_no_retry() {
    let run = RunBuilder::new(1).build();
    let run_id = run.id();
    let job = JobBuilder::new().with_run(run).build();

    let result = terminated(&job, run_id);

    match result.events.as_slice() {
        [JobEvent::RunFailed(failed), JobEvent::JobFailed(job_failed)] => {
            assert_eq!(failed.run_id, run_id);
            assert_eq!(failed.report.reason_code().as_str(), "trigger_terminated");
            assert_eq!(failed.report.kind(), RunFailureKind::Permanent);
            assert_eq!(job_failed.caused_by_run_id, Some(run_id));
        }
        other => panic!("expected a permanent run failure and the job failure, got {other:?}"),
    }
}

#[test]
fn a_run_that_started_before_the_termination_is_left_alone() {
    let run = RunBuilder::new(1).started(ts(5)).build();
    let run_id = run.id();
    let job = JobBuilder::new().with_run(run).build();

    let result = terminated(&job, run_id);

    assert!(result.events.is_empty());
    assert_eq!(
        result.warnings,
        vec![CommandWarning::TriggerTerminationIgnored {
            run_standing: "started"
        }]
    );
}

#[test]
fn a_run_reserved_by_a_claim_is_left_to_the_claim_backstop() {
    let run = RunBuilder::new(1).reserved(ts(5)).build();
    let run_id = run.id();
    let job = JobBuilder::new().with_run(run).build();

    let result = terminated(&job, run_id);

    assert!(result.events.is_empty());
    assert_eq!(
        result.warnings,
        vec![CommandWarning::TriggerTerminationIgnored {
            run_standing: "reserved"
        }]
    );
}

#[test]
fn a_run_that_already_ended_is_left_alone() {
    let ended = [
        RunBuilder::new(1).started(ts(5)).completed(ts(9)),
        RunBuilder::new(1).cancelled(ts(9)),
        RunBuilder::new(1)
            .started(ts(5))
            .failed(ts(9), RunFailureKind::Permanent),
    ];
    for builder in ended {
        let run = builder.build();
        let run_id = run.id();
        let job = JobBuilder::new().with_run(run).build();

        let result = terminated(&job, run_id);

        assert!(result.events.is_empty());
        assert_eq!(result.warnings.len(), 1);
        assert_eq!(result.warnings[0].code(), "fact_discarded_on_terminal_run");
    }
}

#[test]
fn a_run_of_a_cancelled_job_that_has_not_started_still_fails_without_a_second_job_failure() {
    let run = RunBuilder::new(1).build();
    let run_id = run.id();
    let job = JobBuilder::new()
        .with_run(run)
        .with_resolution(JobResolution::cancelled(resolution_id(), ts(20)))
        .build();

    let result = terminated(&job, run_id);

    match result.events.as_slice() {
        [JobEvent::RunFailed(failed)] => {
            assert_eq!(failed.report.reason_code().as_str(), "trigger_terminated");
        }
        other => panic!("expected the run failure alone on a terminal job, got {other:?}"),
    }
}
