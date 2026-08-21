use super::*;

fn at() -> DateTime<Utc> {
    DateTime::from_timestamp(1_700_000_000, 0).unwrap()
}

fn attempts(value: u32) -> MaxAttempts {
    MaxAttempts::new(value).unwrap()
}

#[test]
fn backoff_grows_exponentially_and_stops_at_the_ceiling() {
    // Given: the platform retry policy
    let policy = RetryPolicy::default();
    // When: successive attempts fail
    // Then: the delay multiplies by the factor until the maximum caps it
    assert_eq!(
        policy.backoff_for(AttemptNumber::new(1).unwrap()),
        TimeDelta::seconds(10)
    );
    assert_eq!(
        policy.backoff_for(AttemptNumber::new(2).unwrap()),
        TimeDelta::seconds(30)
    );
    assert_eq!(
        policy.backoff_for(AttemptNumber::new(9).unwrap()),
        TimeDelta::hours(1)
    );
}

#[test]
fn jitter_spreads_the_delay_around_the_backoff() {
    // Given: a policy with a twenty percent jitter span
    let policy = RetryPolicy::default();
    let attempt = AttemptNumber::new(1).unwrap();
    // When: the extremes and the midpoint of the jitter range are drawn
    let low = policy.due_at(at(), attempt, Jitter::from_basis_points(0).unwrap(), None);
    let mid = policy.due_at(at(), attempt, Jitter::MIDPOINT, None);
    let high = policy.due_at(
        at(),
        attempt,
        Jitter::from_basis_points(10_000).unwrap(),
        None,
    );
    // Then: the due time lands within eight to twelve seconds, centred on ten
    assert_eq!(low, at() + TimeDelta::seconds(8));
    assert_eq!(mid, at() + TimeDelta::seconds(10));
    assert_eq!(high, at() + TimeDelta::seconds(12));
}

#[test]
fn a_runner_hint_may_lengthen_the_delay() {
    // Given: a first-attempt backoff of ten seconds
    let policy = RetryPolicy::default();
    // When: the runner asks to be retried no sooner than five minutes
    let due = policy.due_at(
        at(),
        AttemptNumber::FIRST,
        Jitter::MIDPOINT,
        Some(TimeDelta::minutes(5)),
    );
    // Then: the longer of the two wins
    assert_eq!(due, at() + TimeDelta::minutes(5));
}

#[test]
fn a_runner_hint_may_never_shorten_the_delay() {
    // Given: a first-attempt backoff of ten seconds
    let policy = RetryPolicy::default();
    // When: the runner asks to be retried after one second
    let due = policy.due_at(
        at(),
        AttemptNumber::FIRST,
        Jitter::MIDPOINT,
        Some(TimeDelta::seconds(1)),
    );
    // Then: the platform backoff still governs
    assert_eq!(due, at() + TimeDelta::seconds(10));
}

#[test]
fn a_jitter_draw_outside_its_range_is_refused() {
    // Given: a draw expressed in basis points
    // When/Then: only the closed zero-to-one range is admitted
    assert!(Jitter::from_basis_points(-1).is_err());
    assert!(Jitter::from_basis_points(10_001).is_err());
}

#[test]
fn a_default_budget_above_the_ceiling_is_refused_at_construction() {
    // Given: an operator raising the default above the ceiling it must stay under
    // When: the limits are built
    let result = ServiceLimits::new(
        attempts(2),
        attempts(5),
        TimeDelta::hours(1),
        TimeDelta::hours(1),
        TimeDelta::hours(24),
    );
    // Then: the service refuses to hold a configuration whose every job would fail its budget
    assert_eq!(
        result,
        Err(JobsError::MaxAttemptsAboveCeiling {
            requested: 5,
            ceiling: 2
        })
    );
}

#[test]
fn a_non_positive_service_duration_is_refused_at_construction() {
    // Given: durations a deployment could pass as zero or negative
    // When/Then: neither may reach a running service
    assert_eq!(
        ServiceLimits::new(
            attempts(3),
            attempts(1),
            TimeDelta::zero(),
            TimeDelta::hours(1),
            TimeDelta::hours(24)
        ),
        Err(JobsError::InvalidDuration {
            field: "max_run_duration"
        })
    );
    assert_eq!(
        ServiceLimits::new(
            attempts(3),
            attempts(1),
            TimeDelta::hours(1),
            TimeDelta::seconds(-1),
            TimeDelta::hours(24)
        ),
        Err(JobsError::InvalidDuration {
            field: "inactivity_timeout"
        })
    );
    assert_eq!(
        ServiceLimits::new(
            attempts(3),
            attempts(1),
            TimeDelta::hours(1),
            TimeDelta::hours(1),
            TimeDelta::zero()
        ),
        Err(JobsError::InvalidDuration {
            field: "retirement_quiet_period"
        })
    );
}

#[test]
fn a_retry_policy_that_could_never_grow_is_refused_at_construction() {
    // Given: a factor of zero, which would collapse every backoff to nothing
    // When/Then: the policy is refused rather than applied job by job
    assert_eq!(
        RetryPolicy::new(TimeDelta::seconds(1), 0, TimeDelta::hours(1), 2_000),
        Err(JobsError::InvalidRetryFactor)
    );
}

#[test]
fn a_maximum_delay_below_the_base_delay_is_refused_at_construction() {
    // Given: a ceiling under the very first backoff
    // When/Then: the contradiction is refused at boot, not resolved silently at runtime
    assert_eq!(
        RetryPolicy::new(TimeDelta::seconds(10), 3, TimeDelta::seconds(1), 2_000),
        Err(JobsError::InvalidDuration { field: "max_delay" })
    );
}

#[test]
fn a_jitter_span_outside_its_range_is_refused_at_construction() {
    // Given: a jitter span expressed outside the closed basis-point range
    // When/Then: the policy is refused
    assert_eq!(
        RetryPolicy::new(TimeDelta::seconds(10), 3, TimeDelta::hours(1), 20_000),
        Err(JobsError::InvalidJitter)
    );
}

#[test]
fn a_valid_configuration_is_accepted_and_reads_back_its_own_figures() {
    // Given: a deployment lowering every knob to its own scale
    let limits = ServiceLimits::new(
        attempts(3),
        attempts(2),
        TimeDelta::seconds(600),
        TimeDelta::seconds(300),
        TimeDelta::seconds(900),
    )
    .unwrap();
    let policy = RetryPolicy::new(TimeDelta::seconds(2), 3, TimeDelta::minutes(10), 2_000).unwrap();
    // When/Then: the configured figures govern, none of them hard-coded
    assert_eq!(limits.max_run_duration(), TimeDelta::seconds(600));
    assert_eq!(limits.inactivity_timeout(), TimeDelta::seconds(300));
    assert_eq!(limits.retirement_quiet_period(), TimeDelta::seconds(900));
    assert_eq!(limits.budget(None).unwrap().get(), 2);
    assert_eq!(policy.base_delay(), TimeDelta::seconds(2));
    assert_eq!(
        policy.backoff_for(AttemptNumber::FIRST),
        TimeDelta::seconds(2)
    );
}
