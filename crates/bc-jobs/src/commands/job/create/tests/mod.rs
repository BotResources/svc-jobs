use super::*;
use crate::domain::job::resolution::JobResolution;
use crate::domain::requester::Requester;
use crate::fixtures::{
    JobBuilder, RunBuilder, job_id, producer, resolution_id, runner_type, service_passport,
    source_entity_id, ts, user, user_passport,
};
use serde_json::json;

mod guards;
mod parenting;
mod priority;
mod repeat;
mod skeleton;

fn config() -> RunnerConfig {
    RunnerConfig::new(json!({ "prompt": "summarise", "temperature": 0.2 })).unwrap()
}

fn command() -> CreateJob {
    CreateJob {
        id: job_id(),
        runner_type: runner_type(),
        producer: producer(),
        config: None,
        parent_job_id: None,
        triggered_by: None,
        source_entity_id: None,
        max_attempts: None,
        requested_priority: Priority::DEFAULT,
        initial_job_id: None,
        requester: Requester::Service(None),
        passport: Some(service_passport()),
        payload: None,
        call: None,
    }
}

fn queued(outcome: CreateOutcome) -> JobQueued {
    match outcome {
        CreateOutcome::Queued(result) => match result.events.into_iter().next() {
            Some(JobEvent::JobQueued(fact)) => *fact,
            other => panic!("expected a JobQueued fact, got {other:?}"),
        },
        CreateOutcome::AlreadyQueued => panic!("expected the job to be queued"),
    }
}

#[test]
fn a_declared_job_is_queued_under_the_id_its_producer_minted() {
    let command = command();
    let id = command.id;
    let outcome = create_job(command, None, None, None, None, &ServiceLimits::default()).unwrap();
    let fact = queued(outcome);
    assert_eq!(fact.job_id, id);
    assert_eq!(fact.runner_type, runner_type());
    assert_eq!(fact.producer, producer());
    assert_eq!(fact.parent_job_id, None);
}

#[test]
fn the_queued_fact_carries_the_user_the_work_runs_for() {
    let known = user();
    let command = CreateJob {
        triggered_by: Some(known.clone()),
        passport: Some(user_passport(&known)),
        ..command()
    };
    let outcome = create_job(command, None, None, None, None, &ServiceLimits::default()).unwrap();
    assert_eq!(queued(outcome).triggered_by, Some(known));
}

#[test]
fn an_identical_redelivery_of_a_creation_is_absorbed() {
    let command = command();
    let existing = JobBuilder::new().with_id(command.id).build();
    let outcome = create_job(
        command,
        Some(&existing),
        None,
        None,
        None,
        &ServiceLimits::default(),
    )
    .unwrap();
    assert_eq!(outcome, CreateOutcome::AlreadyQueued);
}

#[test]
fn reusing_a_known_id_with_different_input_is_rejected() {
    let existing = JobBuilder::new().build();
    let conflicting = CreateJob {
        id: existing.id(),
        max_attempts: Some(MaxAttempts::new(2).unwrap()),
        ..command()
    };
    let result = create_job(
        conflicting,
        Some(&existing),
        None,
        None,
        None,
        &ServiceLimits::default(),
    );
    assert_eq!(
        result,
        Err(JobsError::JobIdConflict {
            job_id: existing.id().as_uuid()
        })
    );
}

#[test]
fn a_second_live_job_for_one_source_reference_is_rejected() {
    let entity = source_entity_id();
    let active = JobBuilder::new().with_source(entity).build();
    let command = CreateJob {
        source_entity_id: Some(entity),
        ..command()
    };
    let result = create_job(
        command,
        None,
        Some(&active),
        None,
        None,
        &ServiceLimits::default(),
    );
    assert_eq!(
        result,
        Err(JobsError::SourceAlreadyActive {
            active_job_id: active.id().as_uuid()
        })
    );
}

#[test]
fn a_source_whose_previous_job_is_terminal_may_be_declared_again() {
    let entity = source_entity_id();
    let settled = JobBuilder::new()
        .with_source(entity)
        .with_resolution(JobResolution::completed(resolution_id(), ts(30)))
        .build();
    let command = CreateJob {
        source_entity_id: Some(entity),
        ..command()
    };
    let outcome = create_job(
        command,
        None,
        Some(&settled),
        None,
        None,
        &ServiceLimits::default(),
    )
    .unwrap();
    let fact = queued(outcome);
    assert_eq!(fact.source.map(|source| source.entity_id()), Some(entity));
}

#[test]
fn a_budget_above_the_service_ceiling_is_rejected() {
    let command = CreateJob {
        max_attempts: Some(MaxAttempts::new(99).unwrap()),
        ..command()
    };
    let result = create_job(command, None, None, None, None, &ServiceLimits::default());
    assert_eq!(
        result,
        Err(JobsError::MaxAttemptsAboveCeiling {
            requested: 99,
            ceiling: 10
        })
    );
}

#[test]
fn the_queued_fact_carries_the_runner_configuration_the_producer_declared() {
    let command = CreateJob {
        config: Some(config()),
        ..command()
    };
    let outcome = create_job(command, None, None, None, None, &ServiceLimits::default()).unwrap();
    assert_eq!(queued(outcome).config, Some(config()));
}

#[test]
fn an_identical_redelivery_of_a_creation_carrying_a_configuration_is_absorbed() {
    let command = CreateJob {
        config: Some(config()),
        ..command()
    };
    let existing = JobBuilder::new()
        .with_id(command.id)
        .with_config(config())
        .build();
    let outcome = create_job(
        command,
        Some(&existing),
        None,
        None,
        None,
        &ServiceLimits::default(),
    )
    .unwrap();
    assert_eq!(outcome, CreateOutcome::AlreadyQueued);
}

#[test]
fn a_root_job_is_owned_by_the_producer_that_declared_it() {
    let command = command();
    let outcome = create_job(command, None, None, None, None, &ServiceLimits::default()).unwrap();
    assert_eq!(queued(outcome).owner, JobOwner::Producer(producer()));
}
