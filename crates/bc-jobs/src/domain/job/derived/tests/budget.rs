use super::*;

fn lowered_to(ceiling: u32) -> ServiceLimits {
    ServiceLimits::new(
        MaxAttempts::new(ceiling).unwrap(),
        MaxAttempts::new(1).unwrap(),
        chrono::TimeDelta::hours(72),
        chrono::TimeDelta::hours(24),
        chrono::TimeDelta::hours(24),
    )
    .unwrap()
}

#[test]
fn a_job_accepted_above_a_ceiling_since_lowered_still_loads() {
    // Given: a job accepted with five attempts, and an operator who later lowered the ceiling to three
    let state = JobBuilder::new().with_max_attempts(5).state();
    // When: it is read back
    let job = Job::hydrate(state).unwrap();
    // Then: it loads — a service knob never turns accepted history into unreadable data
    assert_eq!(job.max_attempts(), Some(MaxAttempts::new(5).unwrap()));
}

#[test]
fn a_stored_budget_above_a_lowered_ceiling_is_refused_never_narrowed() {
    // Given: a job holding a stored budget of five under a ceiling since lowered to three
    let job = JobBuilder::new().with_max_attempts(5).build();
    // When: the effective budget is read
    let result = job.budget(&lowered_to(3));
    // Then: the contradiction is refused with both figures, rather than silently clamped to three
    assert_eq!(
        result,
        Err(JobsError::MaxAttemptsAboveCeiling {
            requested: 5,
            ceiling: 3
        })
    );
}

#[test]
fn a_job_declaring_no_budget_takes_the_configured_default_whatever_the_ceiling() {
    // Given: a job that declared no budget, under limits whose default is one
    let job = JobBuilder::new().build();
    // When: the effective budget is read
    // Then: the configured default governs — a default above its own ceiling cannot exist,
    // because the limits refuse to be constructed at all
    assert_eq!(job.budget(&lowered_to(1)).unwrap().get(), 1);
}

#[test]
fn a_stored_budget_under_the_ceiling_governs_the_dispatch_bound() {
    // Given: a job that lowered its own budget to two under a ceiling of ten
    let job = JobBuilder::new().with_max_attempts(2).build();
    // When: the effective budget is read
    let budget = job.budget(&ServiceLimits::default()).unwrap();
    // Then: the declared figure is honoured, never widened to the ceiling
    assert_eq!(budget.get(), 2);
    assert!(!budget.allows(AttemptNumber::new(3).unwrap()));
}

#[test]
fn a_job_declaring_no_budget_falls_back_to_the_service_default() {
    // Given: a job that declared no attempt count
    let job = JobBuilder::new().build();
    // When: the effective budget is read
    // Then: the service default governs
    assert_eq!(
        job.budget(&ServiceLimits::default()).unwrap().get(),
        ServiceLimits::default().default_max_attempts().get()
    );
}
