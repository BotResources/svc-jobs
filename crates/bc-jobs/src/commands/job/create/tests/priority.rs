use super::*;
use crate::domain::ids::KnownUserId;
use crate::domain::priority::UserMaxPriority;

fn level(value: i64) -> Priority {
    Priority::new(value).unwrap()
}

fn capped_at(user: &KnownUser, maximum: Option<i64>) -> UserMaxPriority {
    UserMaxPriority::new(KnownUserId::new(user.id().0).unwrap(), maximum.map(level))
}

#[test]
fn a_users_cap_applies_at_admission_and_the_request_is_kept() {
    // Given: a user capped at 3
    let known = user();
    let cap = capped_at(&known, Some(3));
    let command = CreateJob {
        passport: Some(user_passport(&known)),
        triggered_by: Some(known),
        requested_priority: level(5),
        ..command()
    };
    // When: that user's job requesting 5 is admitted
    let fact = queued(
        create_job(
            command,
            None,
            None,
            None,
            Some(&cap),
            &ServiceLimits::default(),
        )
        .unwrap(),
    );
    // Then: it waits at 3, and its request stays 5
    assert_eq!(fact.requested_priority, level(5));
    assert_eq!(fact.effective_priority, level(3));
}

#[test]
fn a_job_created_without_a_priority_requests_three_under_the_default_cap() {
    // Given: a service whose default maximum is 2, and a user with no maximum of their own
    let limits = ServiceLimits::default().with_default_max_priority(level(2));
    let known = user();
    let cap = capped_at(&known, None);
    let command = CreateJob {
        passport: Some(user_passport(&known)),
        triggered_by: Some(known),
        ..command()
    };
    // When: the job is admitted
    let fact = queued(create_job(command, None, None, None, Some(&cap), &limits).unwrap());
    // Then: it requested 3 and the default caps it at 2
    assert_eq!(fact.requested_priority, Priority::DEFAULT);
    assert_eq!(fact.effective_priority, level(2));
}

#[test]
fn a_job_that_runs_for_nobody_is_capped_by_the_service_default() {
    // Given: a service whose default maximum is 4
    let limits = ServiceLimits::default().with_default_max_priority(level(4));
    let command = CreateJob {
        requested_priority: level(5),
        ..command()
    };
    // When: an autonomous job requesting 5 is admitted
    let fact = queued(create_job(command, None, None, None, None, &limits).unwrap());
    // Then: the default caps it
    assert_eq!(fact.effective_priority, level(4));
}

#[test]
fn a_child_waits_at_its_parents_effective_priority_whatever_it_requested() {
    // Given: a parent running at effective priority 5, and a user capped at 1
    let known = user();
    let parent = JobBuilder::new()
        .with_priority(level(5), level(5), None)
        .with_passport(user_passport(&known))
        .with_run(RunBuilder::new(1).started(ts(5)).build())
        .build();
    let cap = capped_at(&known, Some(1));
    let command = CreateJob {
        parent_job_id: Some(parent.id()),
        passport: None,
        triggered_by: Some(known),
        requested_priority: level(1),
        ..command()
    };
    // When: the child is admitted
    let fact = queued(
        create_job(
            command,
            None,
            None,
            Some(&parent),
            Some(&cap),
            &ServiceLimits::default(),
        )
        .unwrap(),
    );
    // Then: it requested and waits at its parent's 5 — its own request is ignored
    assert_eq!(fact.effective_priority, level(5));
    assert_eq!(fact.requested_priority, level(5));
}

#[test]
fn a_child_under_a_forced_parent_is_born_forced_at_the_parents_level() {
    // Given: a parent that requested 2 and a super user forced to 5
    let parent = JobBuilder::new()
        .with_priority(level(2), level(5), Some(level(5)))
        .with_run(RunBuilder::new(1).started(ts(5)).build())
        .build();
    let command = CreateJob {
        parent_job_id: Some(parent.id()),
        requested_priority: level(1),
        ..command()
    };
    // When: a child is admitted under it
    let fact = queued(
        create_job(
            command,
            None,
            None,
            Some(&parent),
            None,
            &ServiceLimits::default(),
        )
        .unwrap(),
    );
    // Then: requested, effective and forced are all the parent's forced 5
    assert_eq!(fact.requested_priority, level(5));
    assert_eq!(fact.effective_priority, level(5));
    assert_eq!(fact.forced_priority, Some(level(5)));
}

#[test]
fn a_child_under_an_unforced_parent_is_not_forced() {
    // Given: a parent at effective 4, not forced
    let parent = JobBuilder::new()
        .with_priority(level(4), level(4), None)
        .with_run(RunBuilder::new(1).started(ts(5)).build())
        .build();
    let command = CreateJob {
        parent_job_id: Some(parent.id()),
        ..command()
    };
    // When: a child is admitted under it
    let fact = queued(
        create_job(
            command,
            None,
            None,
            Some(&parent),
            None,
            &ServiceLimits::default(),
        )
        .unwrap(),
    );
    // Then: it inherits 4 and carries no forced level
    assert_eq!(fact.effective_priority, level(4));
    assert_eq!(fact.forced_priority, None);
}

#[test]
fn a_redelivery_requesting_another_priority_is_a_conflicting_reuse() {
    // Given: a job already created requesting the default
    let existing = JobBuilder::new().build();
    let conflicting = CreateJob {
        id: existing.id(),
        requested_priority: level(5),
        ..command()
    };
    // When: the same id comes back requesting 5
    let result = create_job(
        conflicting,
        Some(&existing),
        None,
        None,
        None,
        &ServiceLimits::default(),
    );
    // Then: it is rejected rather than absorbed
    assert_eq!(
        result,
        Err(JobsError::JobIdConflict {
            job_id: existing.id().as_uuid()
        })
    );
}

#[test]
fn a_redelivered_child_creation_is_absorbed_whatever_priority_it_names() {
    // Given: a child already created under a parent at 5, recorded as requesting 5
    let parent = JobBuilder::new()
        .with_priority(level(5), level(5), None)
        .with_run(RunBuilder::new(1).started(ts(5)).build())
        .build();
    let existing = JobBuilder::new()
        .with_parent(parent.id())
        .with_priority(level(5), level(5), None)
        .build();
    let redelivered = CreateJob {
        id: existing.id(),
        parent_job_id: Some(parent.id()),
        requested_priority: level(1),
        ..command()
    };
    // When: the producer's identical command (asking 1) is delivered again
    let outcome = create_job(
        redelivered,
        Some(&existing),
        None,
        Some(&parent),
        None,
        &ServiceLimits::default(),
    )
    .unwrap();
    // Then: it is absorbed — a child's request is ignored, so it cannot conflict
    assert_eq!(outcome, CreateOutcome::AlreadyQueued);
}
