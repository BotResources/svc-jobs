use super::*;
use crate::domain::job::resolution::JobResolution;
use crate::domain::requester::Requester;
use crate::fixtures::{JobBuilder, RunBuilder, job_id, resolution_id, run_id, ts};

fn finished_origin() -> (Job, RunId) {
    let run = RunBuilder::new(1).started(ts(5)).completed(ts(10)).build();
    let run_id = run.id();
    (JobBuilder::new().with_run(run).build(), run_id)
}

fn command(origin_run_id: RunId) -> RestartJob {
    RestartJob {
        restart_job_id: job_id(),
        origin_run_id,
        applicable_maximum: Priority::DEFAULT,
    }
}

fn queued(result: JobCommandResult) -> JobQueued {
    match result.events.as_slice() {
        [JobEvent::JobQueued(fact)] => (**fact).clone(),
        other => panic!("expected one JobQueued, got {other:?}"),
    }
}

#[test]
fn a_restart_of_a_job_that_is_its_own_parent_is_its_own_parent_and_keeps_the_initial_job() {
    let (origin, run) = finished_origin();
    let fact = queued(restart_job(&origin, ParentContext::Root, command(run)).unwrap());
    assert_eq!(fact.parent_job_id, None);
    assert_eq!(fact.initial_job_id, Some(origin.id()));
    assert_eq!(fact.restart_of_run_id, Some(run));
    assert_eq!(fact.predecessor_job_id, None);
    assert!(fact.source.is_none());
    assert_eq!(fact.producer.as_str(), RESTART_PRODUCER);
}

#[test]
fn a_restart_keeps_the_requester_the_passport_and_the_work_of_its_origin() {
    let requester = Requester::Run(run_id());
    let run = RunBuilder::new(1).started(ts(5)).completed(ts(10)).build();
    let run_id = run.id();
    let origin = JobBuilder::new()
        .with_requester(requester)
        .with_max_attempts(4)
        .with_run(run)
        .build();
    let fact = queued(restart_job(&origin, ParentContext::Root, command(run_id)).unwrap());
    assert_eq!(fact.requester, Some(requester));
    assert_eq!(fact.passport.as_ref(), origin.passport());
    assert_eq!(fact.max_attempts, origin.max_attempts());
    assert_eq!(fact.runner_type, *origin.runner_type());
}

#[test]
fn a_restart_below_a_parent_hangs_where_the_origin_hung_and_inherits_its_priority() {
    let parent = JobBuilder::new()
        .with_run(RunBuilder::new(1).started(ts(1)).build())
        .build();
    let run = RunBuilder::new(1).started(ts(5)).completed(ts(10)).build();
    let run_id = run.id();
    let origin = JobBuilder::new()
        .with_parent(parent.id())
        .with_initial_job(parent.initial_job_id())
        .with_run(run)
        .build();
    let fact =
        queued(restart_job(&origin, ParentContext::Loaded(&parent), command(run_id)).unwrap());
    assert_eq!(fact.parent_job_id, Some(parent.id()));
    assert_eq!(fact.initial_job_id, Some(parent.initial_job_id()));
    assert_eq!(fact.effective_priority, parent.priority().effective());
}

#[test]
fn a_restart_under_a_terminal_parent_is_refused() {
    let parent = JobBuilder::new()
        .with_resolution(JobResolution::completed(resolution_id(), ts(30)))
        .build();
    let run = RunBuilder::new(1).started(ts(5)).completed(ts(10)).build();
    let run_id = run.id();
    let origin = JobBuilder::new()
        .with_parent(parent.id())
        .with_initial_job(parent.initial_job_id())
        .with_run(run)
        .build();
    assert_eq!(
        restart_job(&origin, ParentContext::Loaded(&parent), command(run_id)).err(),
        Some(JobsError::ParentJobTerminal {
            parent_job_id: parent.id().as_uuid()
        })
    );
}

#[test]
fn a_restart_of_a_run_the_origin_never_had_is_refused() {
    let (origin, _) = finished_origin();
    assert!(restart_job(&origin, ParentContext::Root, command(run_id())).is_err());
}

#[test]
fn a_manual_retry_successor_of_a_restart_job_is_a_restart_of_the_same_run() {
    use crate::commands::job::manual_retry::{ManualRetryJob, ManualRetryOutcome, manual_retry};
    use crate::domain::fleet::dispatch_mode::SuccessorDispatch;
    use crate::domain::job::resolution::JobFailureCause;
    use crate::fixtures::{manual_retry_id, user};
    let (origin, run) = finished_origin();
    let fact = queued(restart_job(&origin, ParentContext::Root, command(run)).unwrap());
    let resolution = resolution_id();
    let failed_restart = JobBuilder::new()
        .with_id(fact.job_id)
        .with_initial_job(origin.initial_job_id())
        .with_restart_of(run)
        .with_resolution(
            JobResolution::failed(resolution, ts(30), JobFailureCause::DeclaredByOwner, None)
                .unwrap(),
        )
        .build();
    let outcome = manual_retry(
        &failed_restart,
        ParentContext::Root,
        None,
        ManualRetryJob {
            manual_retry_id: manual_retry_id(),
            failed_resolution_id: resolution,
            successor_job_id: job_id(),
            dispatch: SuccessorDispatch::Wait,
            requested_by: user(),
            applicable_maximum: Priority::HIGHEST,
        },
    )
    .unwrap();
    let ManualRetryOutcome::Started(plan) = outcome else {
        panic!("expected the retry to start");
    };
    assert_eq!(queued(plan.successor).restart_of_run_id, Some(run));
}
