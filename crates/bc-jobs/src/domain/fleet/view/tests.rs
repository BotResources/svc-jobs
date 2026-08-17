use super::*;
use crate::domain::fleet::RunnerTypeState;
use crate::domain::fleet::capacity::Capacity;
use crate::domain::fleet::instance::RunnerInstanceState;
use crate::domain::fleet::status::ReportedStatus;
use crate::domain::ids::{PresenceSessionId, RunnerTypeId};
use crate::domain::job::resolution::JobResolution;
use crate::domain::keys::{InstanceKey, RunnerTypeKey, RunnerVersion};
use crate::domain::run::failure::RunFailureKind;
use crate::fixtures::{JobBuilder, RunBuilder, resolution_id, ts};
use uuid::Uuid;

fn live(key: &str, reported_status: ReportedStatus) -> RunnerInstance {
    declaring(key, reported_status, 1)
}

fn declaring(key: &str, reported_status: ReportedStatus, capacity: u32) -> RunnerInstance {
    RunnerInstance::hydrate(RunnerInstanceState {
        key: InstanceKey::new(key).unwrap(),
        session_id: PresenceSessionId::new(Uuid::now_v7()).unwrap(),
        version: RunnerVersion::new("1.4.2").unwrap(),
        reported_status,
        capacity: Capacity::new(capacity).unwrap(),
        connected_at: ts(1),
        last_observed_at: ts(6),
        status_change_number: 0,
    })
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
    let view = fleet_view(
        &analysts(vec![live("pod-7", ReportedStatus::Ready)]),
        &[job],
    );
    // Then: the instance reads as busy and carries exactly that run
    let load = view.instances().first().unwrap();
    assert!(load.is_busy());
    assert_eq!(load.current_run_ids(), [run_id]);
    assert_eq!(view.busy_instance_count(), 1);
    assert_eq!(view.idle_instance_count(), 0);
}

#[test]
fn an_instance_is_busy_only_once_its_runs_reach_the_capacity_it_declared() {
    // Given: an instance that declared room for two runs, carrying one
    let job = JobBuilder::new()
        .with_run(
            RunBuilder::new(1)
                .started_on(analyst("pod-7"), ts(5))
                .build(),
        )
        .build();
    let view = fleet_view(
        &analysts(vec![declaring("pod-7", ReportedStatus::Ready, 2)]),
        &[job],
    );
    // Then: one run out of two is not busy — occupation is measured against the declaration
    let load = view.instances().first().unwrap();
    assert_eq!(load.capacity(), Capacity::new(2).unwrap());
    assert!(!load.is_busy());
    assert_eq!(view.busy_instance_count(), 0);
    assert_eq!(view.idle_instance_count(), 1);
}

#[test]
fn a_type_declares_the_capacity_of_its_live_instances_that_still_take_work() {
    // Given: two instances taking work and one winding down
    let view = fleet_view(
        &analysts(vec![
            declaring("pod-7", ReportedStatus::Ready, 2),
            declaring("pod-8", ReportedStatus::Ready, 3),
            declaring("pod-9", ReportedStatus::Draining, 4),
        ]),
        &[],
    );
    // Then: the declared total is what could still be taken, so a draining instance
    // contributes nothing even though it is still live
    assert_eq!(view.total_capacity(), 5);
    assert_eq!(view.instances().len(), 3);
}

#[test]
fn an_unregistered_type_declares_no_capacity_at_all() {
    // Given: a runner type key no instance ever announced
    let view = unregistered_fleet_view(&RunnerTypeKey::new("archivist").unwrap(), &[]);
    // Then: nothing is declared — an absent fleet never reads as room for work
    assert_eq!(view.total_capacity(), 0);
}

#[test]
fn an_instance_reporting_draining_while_carrying_no_run_is_idle() {
    // Given: an instance winding down by its own report, with no run of its own
    let view = fleet_view(
        &analysts(vec![live("pod-7", ReportedStatus::Draining)]),
        &[],
    );
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
        &analysts(vec![
            live("pod-7", ReportedStatus::Draining),
            live("pod-9", ReportedStatus::Ready),
        ]),
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
    let load = instance_load(&analyst("pod-7"), Capacity::new(1).unwrap(), &[job]);
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
        &analysts(vec![live("pod-7", ReportedStatus::Ready)]),
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
    let view = fleet_view(
        &analysts(vec![live("pod-7", ReportedStatus::Draining)]),
        &[job],
    );
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
    let view = fleet_view(
        &analysts(vec![live("pod-7", ReportedStatus::Ready)]),
        &[job],
    );
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
    let view = fleet_view(
        &analysts(vec![live("pod-7", ReportedStatus::Ready)]),
        &[job],
    );
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
        &analysts(vec![live("pod-7", ReportedStatus::Ready)]),
        &[scribe, analyst_job],
    );
    // Then: only the analyst work is counted — a type never reads another's queue
    assert_eq!(view.waiting_job_count(), 1);
}
