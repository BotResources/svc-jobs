use super::*;
use crate::domain::keys::{DisplayName, ProducerKey};
use crate::domain::references::KnownUser;

fn erased_job_like(command: &CreateJob) -> JobBuilder {
    JobBuilder::new()
        .with_id(command.id)
        .with_resolution(JobResolution::cancelled(resolution_id(), ts(100)))
        .with_request_data_erased(ts(100))
}

fn declared_with_content() -> CreateJob {
    CreateJob {
        config: Some(config()),
        payload: None,
        ..command()
    }
}

#[test]
fn a_create_repeated_after_the_erasure_of_its_request_data_is_the_same_declaration() {
    let command = declared_with_content();
    let existing = erased_job_like(&command).build();
    assert!(command.is_the_same_declaration_as(&existing));
}

#[test]
fn a_create_repeated_before_any_erasure_must_carry_the_same_content() {
    let command = declared_with_content();
    let existing = JobBuilder::new().with_id(command.id).build();
    assert!(!command.is_the_same_declaration_as(&existing));
    let faithful = JobBuilder::new()
        .with_id(command.id)
        .with_config(config())
        .build();
    assert!(command.is_the_same_declaration_as(&faithful));
}

#[test]
fn an_erased_job_still_tells_another_runner_type_from_the_same_declaration() {
    let command = declared_with_content();
    let existing = erased_job_like(&command)
        .with_runner_type("another-type")
        .build();
    assert!(!command.is_the_same_declaration_as(&existing));
}

#[test]
fn an_erased_job_still_tells_another_maximum_of_attempts_from_the_same_declaration() {
    let command = declared_with_content();
    let existing = erased_job_like(&command).with_max_attempts(2).build();
    assert!(!command.is_the_same_declaration_as(&existing));
}

#[test]
fn an_erased_job_still_tells_another_requester_from_the_same_declaration() {
    let command = declared_with_content();
    let existing = erased_job_like(&command)
        .with_requester(Requester::Run(crate::fixtures::run_id()))
        .build();
    assert!(!command.is_the_same_declaration_as(&existing));
}

#[test]
fn an_erased_job_compares_the_user_by_identity_and_not_by_the_name_shown() {
    let known = user();
    let command = CreateJob {
        triggered_by: Some(known.clone()),
        ..declared_with_content()
    };
    let renamed = KnownUser::new(known.id(), DisplayName::new("erased").unwrap()).unwrap();
    let existing = erased_job_like(&command).with_triggered_by(renamed).build();
    assert!(command.is_the_same_declaration_as(&existing));
    let other = erased_job_like(&command).with_triggered_by(user()).build();
    assert!(!command.is_the_same_declaration_as(&other));
}

#[test]
fn an_erased_job_still_tells_another_producer_from_the_same_declaration() {
    let command = declared_with_content();
    let other = ProducerKey::new("someone-else").unwrap();
    let different = CreateJob {
        producer: other,
        ..command.clone()
    };
    let existing = erased_job_like(&command).build();
    assert!(!different.is_the_same_declaration_as(&existing));
}
