use super::*;

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
    assert_eq!(
        fact.owner,
        JobOwner::Runner {
            parent_job_id: parent.id(),
            runner_type: parent.runner_type().clone()
        }
    );
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
