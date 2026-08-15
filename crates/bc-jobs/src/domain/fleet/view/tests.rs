use super::*;
use crate::domain::fleet::RunnerTypeState;
use crate::domain::ids::{PresenceSessionId, RunnerTypeId};
use crate::domain::job::resolution::JobResolution;
use crate::domain::keys::{InstanceKey, ReportedStatus, RunnerTypeKey, RunnerVersion};
use crate::domain::run::failure::RunFailureKind;
use crate::fixtures::{JobBuilder, RunBuilder, resolution_id, ts};
use uuid::Uuid;

fn live(key: &str, reported_status: &str) -> RunnerInstance {
    RunnerInstance::hydrate(
        InstanceKey::new(key).unwrap(),
        PresenceSessionId::new(Uuid::now_v7()).unwrap(),
        RunnerVersion::new("1.4.2").unwrap(),
        ReportedStatus::new(reported_status).unwrap(),
        ts(1),
        ts(6),
        0,
    )
    .unwrap()
}

fn analysts(instances: Vec<RunnerInstance>) -> RunnerType {
    RunnerType::hydrate(RunnerTypeState {
        id: RunnerTypeId::new(Uuid::now_v7()).unwrap(),
        key: RunnerTypeKey::new("analyst").unwrap(),
        registered_at: ts(0),
        instances,
    })
    .unwrap()
}

fn analyst(key: &str) -> RunnerInstanceReference {
    RunnerInstanceReference::new(
        RunnerTypeKey::new("analyst").unwrap(),
        InstanceKey::new(key).unwrap(),
    )
}

#[test]
fn an_instance_executing_a_run_is_busy_and_names_the_run_it_carries() {
    // Given: a job whose run pod-7 started
    let run = RunBuilder::new(1)
        .started_on(analyst("pod-7"), ts(5))
        .build();
    let run_id = run.id();
    let job = JobBuilder::new().with_run(run).build();
    // When: the fleet view is derived
    let view = fleet_view(&analysts(vec![live("pod-7", "idle")]), &[job]);
    // Then: the instance reads as busy and carries exactly that run
    let load = view.instances().first().unwrap();
    assert!(load.is_busy());
    assert_eq!(load.current_run_ids(), [run_id]);
    assert_eq!(view.busy_instance_count(), 1);
    assert_eq!(view.idle_instance_count(), 0);
}

#[test]
fn an_instance_reporting_busy_while_carrying_no_run_is_idle() {
    // Given: an instance whose self-reported status says busy, with no run of its own
    let view = fleet_view(&analysts(vec![live("pod-7", "busy")]), &[]);
    // When/Then: occupation is derived from the runs Jobs holds, never from the report
    assert!(!view.instances().first().unwrap().is_busy());
    assert_eq!(view.busy_instance_count(), 0);
    assert_eq!(view.idle_instance_count(), 1);
}

#[test]
fn the_fleet_splits_its_live_instances_between_busy_and_idle() {
    // Given: two live instances, only one of which started a run
    let job = JobBuilder::new()
        .with_run(
            RunBuilder::new(1)
                .started_on(analyst("pod-7"), ts(5))
                .build(),
        )
        .build();
    // When: the fleet view is derived
    let view = fleet_view(
        &analysts(vec![live("pod-7", "busy"), live("pod-9", "idle")]),
        &[job],
    );
    // Then: the split is exhaustive — every live instance is either busy or idle
    assert_eq!(view.busy_instance_count(), 1);
    assert_eq!(view.idle_instance_count(), 1);
    assert_eq!(view.instances().len(), 2);
}

#[test]
fn a_run_carried_by_a_sibling_instance_never_lands_on_this_one() {
    // Given: a run started by pod-9
    let job = JobBuilder::new()
        .with_run(
            RunBuilder::new(1)
                .started_on(analyst("pod-9"), ts(5))
                .build(),
        )
        .build();
    // When: pod-7's load is derived
    let load = instance_load(&analyst("pod-7"), &[job]);
    // Then: pod-7 carries nothing
    assert!(!load.is_busy());
    assert!(load.current_run_ids().is_empty());
}

#[test]
fn only_a_job_whose_next_attempt_is_still_undispatched_is_waiting() {
    // Given: one job with a dispatched but unclaimed run, and one with no run at all
    let dispatched = JobBuilder::new()
        .with_run(RunBuilder::new(1).build())
        .build();
    let queued = JobBuilder::new().build();
    // When: the fleet view is derived
    let view = fleet_view(
        &analysts(vec![live("pod-7", "idle")]),
        &[dispatched, queued],
    );
    // Then: only the job whose work has not been handed to the type yet is waiting —
    // a dispatched trigger is already the type's, and it is not executing until claimed
    assert_eq!(view.waiting_job_count(), 1);
    assert_eq!(view.executing_job_count(), 0);
}

#[test]
fn a_job_whose_run_an_instance_started_is_executing() {
    // Given: a job whose run pod-7 started
    let job = JobBuilder::new()
        .with_run(
            RunBuilder::new(1)
                .started_on(analyst("pod-7"), ts(5))
                .build(),
        )
        .build();
    // When: the fleet view is derived
    let view = fleet_view(&analysts(vec![live("pod-7", "busy")]), &[job]);
    // Then: it left the queue and counts as executing
    assert_eq!(view.waiting_job_count(), 0);
    assert_eq!(view.executing_job_count(), 1);
}

#[test]
fn a_job_awaiting_its_scheduled_retry_waits_again() {
    // Given: a job whose attempt failed transiently and whose retry is scheduled
    let job = JobBuilder::new()
        .with_run(
            RunBuilder::new(1)
                .started_on(analyst("pod-7"), ts(5))
                .failed(ts(20), RunFailureKind::Transient)
                .retry_due(ts(80))
                .build(),
        )
        .build();
    // When: the fleet view is derived
    let view = fleet_view(&analysts(vec![live("pod-7", "idle")]), &[job]);
    // Then: the job is back to waiting and the instance it used is free again
    assert_eq!(view.waiting_job_count(), 1);
    assert_eq!(view.executing_job_count(), 0);
    assert_eq!(view.busy_instance_count(), 0);
}

#[test]
fn a_settled_job_counts_neither_as_waiting_nor_as_executing() {
    // Given: a job that reached a terminal resolution
    let job = JobBuilder::new()
        .with_resolution(JobResolution::completed(resolution_id(), ts(30)))
        .build();
    // When: the fleet view is derived
    let view = fleet_view(&analysts(vec![live("pod-7", "idle")]), &[job]);
    // Then: finished work never inflates the queue
    assert_eq!(view.waiting_job_count(), 0);
    assert_eq!(view.executing_job_count(), 0);
}

#[test]
fn the_jobs_of_another_runner_type_never_reach_these_counts() {
    // Given: a waiting scribe job alongside a waiting analyst job
    let scribe = JobBuilder::new().with_runner_type("scribe").build();
    let analyst_job = JobBuilder::new().build();
    // When: the analyst fleet view is derived over both
    let view = fleet_view(
        &analysts(vec![live("pod-7", "idle")]),
        &[scribe, analyst_job],
    );
    // Then: only the analyst work is counted — a type never reads another's queue
    assert_eq!(view.waiting_job_count(), 1);
}
