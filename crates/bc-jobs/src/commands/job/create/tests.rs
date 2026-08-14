use super::*;
use crate::domain::job::resolution::JobResolution;
use crate::fixtures::{
    JobBuilder, RunBuilder, job_id, producer, resolution_id, runner_type, source_entity_id, ts,
    user,
};

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
    }
}

fn queued(outcome: CreateOutcome) -> JobQueued {
    match outcome {
        CreateOutcome::Queued(result) => match result.events.into_iter().next() {
            Some(JobEvent::JobQueued(fact)) => fact,
            other => panic!("expected a JobQueued fact, got {other:?}"),
        },
        CreateOutcome::AlreadyQueued => panic!("expected the job to be queued"),
    }
}

#[test]
fn a_declared_job_is_queued_under_the_id_its_producer_minted() {
    // Given: a producer declaring a job with its own uuid
    let command = command();
    let id = command.id;
    // When: the declaration is accepted
    let outcome = create_job(command, None, None, None, &ServiceLimits::default()).unwrap();
    // Then: one queued fact carries the caller-supplied id and its routing key
    let fact = queued(outcome);
    assert_eq!(fact.job_id, id);
    assert_eq!(fact.runner_type, runner_type());
    assert_eq!(fact.producer, producer());
    assert_eq!(fact.parent_job_id, None);
}

#[test]
fn the_queued_fact_carries_the_user_the_work_runs_for() {
    // Given: a job declared on behalf of a known user
    let known = user();
    let command = CreateJob {
        triggered_by: Some(known.clone()),
        ..command()
    };
    // When: it is queued
    let outcome = create_job(command, None, None, None, &ServiceLimits::default()).unwrap();
    // Then: the fact carries the user, so no subscriber has to look it up
    assert_eq!(queued(outcome).triggered_by, Some(known));
}

#[test]
fn an_identical_redelivery_of_a_creation_is_absorbed() {
    // Given: a job already created from this exact declaration
    let command = command();
    let existing = JobBuilder::new().with_id(command.id).build();
    // When: the same command is delivered again
    let outcome = create_job(
        command,
        Some(&existing),
        None,
        None,
        &ServiceLimits::default(),
    )
    .unwrap();
    // Then: no duplicate job and no duplicate history
    assert_eq!(outcome, CreateOutcome::AlreadyQueued);
}

#[test]
fn reusing_a_known_id_with_different_input_is_rejected() {
    // Given: a job already created under this id
    let existing = JobBuilder::new().build();
    let conflicting = CreateJob {
        id: existing.id(),
        max_attempts: Some(MaxAttempts::new(2).unwrap()),
        ..command()
    };
    // When: the same id is reused with a different declaration
    let result = create_job(
        conflicting,
        Some(&existing),
        None,
        None,
        &ServiceLimits::default(),
    );
    // Then: the producer gets a definite rejection, not a silent second job
    assert_eq!(
        result,
        Err(JobsError::JobIdConflict {
            job_id: existing.id().as_uuid()
        })
    );
}

#[test]
fn a_second_live_job_for_one_source_reference_is_rejected() {
    // Given: a non-terminal job already covering this source entity
    let entity = source_entity_id();
    let active = JobBuilder::new().with_source(entity).build();
    let command = CreateJob {
        source_entity_id: Some(entity),
        ..command()
    };
    // When: another job is declared for the same source
    let result = create_job(
        command,
        None,
        Some(&active),
        None,
        &ServiceLimits::default(),
    );
    // Then: the duplicate is rejected, naming the job that holds the source
    assert_eq!(
        result,
        Err(JobsError::SourceAlreadyActive {
            active_job_id: active.id().as_uuid()
        })
    );
}

#[test]
fn a_source_whose_previous_job_is_terminal_may_be_declared_again() {
    // Given: the previous job for this source reached a terminal state
    let entity = source_entity_id();
    let settled = JobBuilder::new()
        .with_source(entity)
        .with_resolution(JobResolution::completed(resolution_id(), ts(30)))
        .build();
    let command = CreateJob {
        source_entity_id: Some(entity),
        ..command()
    };
    // When: a new job is declared for the same source
    let outcome = create_job(
        command,
        None,
        Some(&settled),
        None,
        &ServiceLimits::default(),
    )
    .unwrap();
    // Then: it is accepted and carries the source reference
    let fact = queued(outcome);
    assert_eq!(fact.source.map(|source| source.entity_id()), Some(entity));
}

#[test]
fn a_budget_above_the_service_ceiling_is_rejected() {
    // Given: a producer asking for more attempts than the platform allows
    let command = CreateJob {
        max_attempts: Some(MaxAttempts::new(99).unwrap()),
        ..command()
    };
    // When: the declaration is judged
    let result = create_job(command, None, None, None, &ServiceLimits::default());
    // Then: it is rejected with both figures
    assert_eq!(
        result,
        Err(JobsError::MaxAttemptsAboveCeiling {
            requested: 99,
            ceiling: 10
        })
    );
}

#[test]
fn a_child_job_is_owned_by_the_runner_of_its_parent() {
    // Given: a live parent job executed by a runner
    let parent = JobBuilder::new()
        .with_run(RunBuilder::new(1).build())
        .build();
    let command = CreateJob {
        parent_job_id: Some(parent.id()),
        ..command()
    };
    // When: the runner declares a child
    let outcome = create_job(
        command,
        None,
        None,
        Some(&parent),
        &ServiceLimits::default(),
    )
    .unwrap();
    // Then: the child records its parent, whose runner becomes its owner
    let fact = queued(outcome);
    assert_eq!(fact.parent_job_id, Some(parent.id()));
}

#[test]
fn a_child_of_a_terminal_parent_is_rejected() {
    // Given: a parent job that already resolved
    let parent = JobBuilder::new()
        .with_resolution(JobResolution::cancelled(resolution_id(), ts(30)))
        .build();
    let command = CreateJob {
        parent_job_id: Some(parent.id()),
        ..command()
    };
    // When: a child is declared under it
    let result = create_job(
        command,
        None,
        None,
        Some(&parent),
        &ServiceLimits::default(),
    );
    // Then: no work is spawned beneath finished work
    assert_eq!(
        result,
        Err(JobsError::ParentJobTerminal {
            parent_job_id: parent.id().as_uuid()
        })
    );
}

#[test]
fn a_child_naming_an_unknown_parent_is_rejected() {
    // Given: a declaration pointing at a parent this service never saw
    let missing = job_id();
    let command = CreateJob {
        parent_job_id: Some(missing),
        ..command()
    };
    // When: it is judged with no parent loaded
    let result = create_job(command, None, None, None, &ServiceLimits::default());
    // Then: the producer gets a definite rejection rather than an orphan
    assert_eq!(
        result,
        Err(JobsError::ParentJobUnknown {
            parent_job_id: missing.as_uuid()
        })
    );
}

#[test]
fn a_job_declaring_itself_as_its_own_parent_is_rejected() {
    // Given: a declaration whose parent is the job being created
    let id = job_id();
    let command = CreateJob {
        id,
        parent_job_id: Some(id),
        ..command()
    };
    // When: it is judged
    let result = create_job(command, None, None, None, &ServiceLimits::default());
    // Then: the cycle is refused
    assert_eq!(
        result,
        Err(JobsError::SelfReference {
            field: "parent_job_id"
        })
    );
}
