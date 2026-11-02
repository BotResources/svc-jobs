use super::*;
use crate::domain::job::JobCall;
use crate::domain::payload::CallMode;
use uuid::Uuid;

fn erased_like(command: &CreateJob) -> JobBuilder {
    JobBuilder::new()
        .with_id(command.id)
        .with_resolution(JobResolution::cancelled(resolution_id(), ts(100)))
        .with_request_data_erased(ts(100))
}

fn level(value: i64) -> Priority {
    Priority::new(value).unwrap()
}

fn run_command() -> CreateJob {
    CreateJob {
        requester: Requester::Run(crate::fixtures::run_id()),
        call: Some(JobCall {
            mode: CallMode::Sync,
            caller_address: None,
        }),
        ..command()
    }
}

fn run_job_like(command: &CreateJob, call: JobCall) -> JobBuilder {
    let Requester::Run(run) = command.requester else {
        panic!("a run command");
    };
    JobBuilder::new()
        .with_id(command.id)
        .with_requester(Requester::Run(run))
        .with_call(call)
}

#[test]
fn a_create_of_a_child_job_is_the_same_declaration_whatever_priority_it_asked_for() {
    let parent = JobBuilder::new().build();
    let command = CreateJob {
        parent_job_id: Some(parent.id()),
        requested_priority: level(5),
        ..command()
    };
    let existing = erased_like(&command).with_parent(parent.id()).build();
    assert!(command.is_the_same_declaration_as(&existing));
}

#[test]
fn a_create_names_the_parent_it_was_declared_under() {
    let parent = JobBuilder::new().build();
    let command = CreateJob {
        parent_job_id: Some(parent.id()),
        ..command()
    };
    let orphan = erased_like(&command).build();
    assert!(!command.is_the_same_declaration_as(&orphan));
    let other_parent = erased_like(&command).with_parent(job_id()).build();
    assert!(!command.is_the_same_declaration_as(&other_parent));
}

#[test]
fn a_create_names_the_source_it_was_declared_for() {
    let entity = source_entity_id();
    let command = CreateJob {
        source_entity_id: Some(entity),
        ..command()
    };
    let existing = erased_like(&command).with_source(entity).build();
    assert!(command.is_the_same_declaration_as(&existing));
    let other = erased_like(&command)
        .with_source(source_entity_id())
        .build();
    assert!(!command.is_the_same_declaration_as(&other));
    let none = erased_like(&command).build();
    assert!(!command.is_the_same_declaration_as(&none));
}

#[test]
fn a_create_of_a_root_job_names_the_priority_it_asked_for() {
    let command = CreateJob {
        requested_priority: level(3),
        ..command()
    };
    let same = erased_like(&command)
        .with_priority(level(3), level(3), None)
        .build();
    assert!(command.is_the_same_declaration_as(&same));
    let other = erased_like(&command)
        .with_priority(level(4), level(4), None)
        .build();
    assert!(!command.is_the_same_declaration_as(&other));
}

#[test]
fn a_create_that_names_an_initial_job_must_name_the_one_of_the_job() {
    let initial = job_id();
    let parent = JobBuilder::new().with_id(initial).build();
    let command = CreateJob {
        parent_job_id: Some(initial),
        initial_job_id: Some(initial),
        ..command()
    };
    let existing = erased_like(&command).with_parent(initial).build();
    assert!(command.is_the_same_declaration_as(&existing));
    let elsewhere = erased_like(&command)
        .with_parent(initial)
        .with_initial_job(job_id())
        .build();
    assert!(!command.is_the_same_declaration_as(&elsewhere));
    assert_eq!(parent.id(), initial);
}

#[test]
fn a_create_that_names_no_initial_job_matches_whichever_the_job_has() {
    let command = command();
    let existing = erased_like(&command).with_initial_job(job_id()).build();
    assert!(command.is_the_same_declaration_as(&existing));
}

#[test]
fn a_create_by_a_run_stores_the_call_it_declared() {
    let command = run_command();
    let sync = JobCall {
        mode: CallMode::Sync,
        caller_address: None,
    };
    assert!(command.is_the_same_declaration_as(&run_job_like(&command, sync).build()));
}

#[test]
fn a_create_by_a_run_with_another_call_mode_is_not_the_same_declaration() {
    let address = Uuid::now_v7();
    let command = CreateJob {
        call: Some(JobCall {
            mode: CallMode::Background,
            caller_address: Some(address),
        }),
        ..run_command()
    };
    let stored = JobCall {
        mode: CallMode::Sync,
        caller_address: None,
    };
    assert!(!command.is_the_same_declaration_as(&run_job_like(&command, stored).build()));
}

#[test]
fn a_create_by_a_run_with_another_caller_address_is_not_the_same_declaration() {
    let command = CreateJob {
        call: Some(JobCall {
            mode: CallMode::Background,
            caller_address: Some(Uuid::now_v7()),
        }),
        ..run_command()
    };
    let stored = JobCall {
        mode: CallMode::Background,
        caller_address: Some(Uuid::now_v7()),
    };
    assert!(!command.is_the_same_declaration_as(&run_job_like(&command, stored).build()));
    let same = JobCall {
        mode: CallMode::Background,
        caller_address: command.call.unwrap().caller_address,
    };
    assert!(command.is_the_same_declaration_as(&run_job_like(&command, same).build()));
}

#[test]
fn the_call_is_compared_after_the_erasure_of_the_request_data_too() {
    let command = CreateJob {
        call: Some(JobCall {
            mode: CallMode::Background,
            caller_address: Some(Uuid::now_v7()),
        }),
        ..run_command()
    };
    let stored = JobCall {
        mode: CallMode::Sync,
        caller_address: None,
    };
    let existing = run_job_like(&command, stored)
        .with_resolution(JobResolution::cancelled(resolution_id(), ts(100)))
        .with_request_data_erased(ts(100))
        .build();
    assert!(!command.is_the_same_declaration_as(&existing));
}

#[test]
fn a_create_by_a_run_without_a_call_mode_is_never_the_same_declaration() {
    let command = CreateJob {
        call: None,
        ..run_command()
    };
    let stored = JobCall {
        mode: CallMode::Sync,
        caller_address: None,
    };
    assert!(!command.is_the_same_declaration_as(&run_job_like(&command, stored).build()));
}

#[test]
fn a_create_repeated_with_a_refreshed_display_name_is_the_same_declaration() {
    let known = user();
    let command = CreateJob {
        triggered_by: Some(known.clone()),
        ..command()
    };
    let refreshed = crate::domain::references::KnownUser::new(
        known.id(),
        crate::domain::keys::DisplayName::new("renamed").unwrap(),
    )
    .unwrap();
    let existing = JobBuilder::new()
        .with_id(command.id)
        .with_triggered_by(refreshed)
        .build();
    assert!(command.is_the_same_declaration_as(&existing));
}
