use super::*;

fn declared_by(actor: ActorRef) -> DeclarationClaim {
    DeclarationClaim::new(Some(actor), actor)
}

fn calling_run(run: crate::domain::ids::RunId) -> (Caller, ActsFor) {
    (
        Caller::Run {
            run_id: run,
            works_the_job: false,
        },
        ActsFor::default(),
    )
}

#[test]
fn the_owner_of_a_job_without_requester_repeating_a_finish_gets_the_completion_again() {
    let first = command();
    let job = completed_by(first);
    let owner = ActorRef::new(Uuid::now_v7());
    let claim = ResolutionRequester::LegacyOwner(declared_by(owner));
    assert!(matches!(
        job.repeat_of_finish(first, &claim),
        Ok(Some(JobEvent::JobCompleted(_)))
    ));
}

#[test]
fn the_owner_of_a_job_without_requester_repeating_a_fail_gets_the_failure_again() {
    let first = command();
    let job = failed_by(first);
    let owner = ActorRef::new(Uuid::now_v7());
    let claim = ResolutionRequester::LegacyOwner(declared_by(owner));
    assert!(matches!(
        job.repeat_of_fail(first, &claim),
        Ok(Some(JobEvent::JobFailed(_)))
    ));
}

#[test]
fn the_run_that_requested_a_job_repeating_a_resolution_gets_the_first_outcome_again() {
    let first = command();
    let requesting = run_id();
    let (caller, acts_for) = calling_run(requesting);
    let resolution = ResolutionRequester::Caller {
        caller,
        acts_for: acts_for.clone(),
    };
    let finished = JobBuilder::new()
        .with_requester(Requester::Run(requesting))
        .with_resolution(JobResolution::completed(resolution_id(), ts(30)).remembering(Some(first)))
        .build();
    assert!(matches!(
        finished.repeat_of_finish(first, &resolution),
        Ok(Some(JobEvent::JobCompleted(_)))
    ));
    let cancelled = JobBuilder::new()
        .with_requester(Requester::Run(requesting))
        .with_resolution(JobResolution::cancelled(resolution_id(), ts(30)).remembering(Some(first)))
        .build();
    assert!(matches!(
        cancelled.repeat_of_cancel(first, &CancelRequester::Caller { caller, acts_for }),
        Ok(Some(JobEvent::JobCancelled(_)))
    ));
}

#[test]
fn the_service_that_requested_a_job_repeating_a_resolution_gets_the_first_outcome_again() {
    let first = command();
    let service = ActorRef::new(Uuid::now_v7());
    let job = JobBuilder::new()
        .with_requester(Requester::Service(Some(service)))
        .with_resolution(JobResolution::completed(resolution_id(), ts(30)).remembering(Some(first)))
        .build();
    let caller = ResolutionRequester::Caller {
        caller: Caller::Service(service),
        acts_for: ActsFor::default(),
    };
    assert!(matches!(
        job.repeat_of_finish(first, &caller),
        Ok(Some(JobEvent::JobCompleted(_)))
    ));
    let other = ResolutionRequester::Caller {
        caller: Caller::Service(ActorRef::new(Uuid::now_v7())),
        acts_for: ActsFor::default(),
    };
    assert_eq!(
        job.repeat_of_finish(first, &other),
        Err(JobsError::NotRequester)
    );
}

#[test]
fn the_rule_that_finishes_a_called_job_gets_the_first_finish_again_but_never_a_first_failure() {
    use crate::domain::ownership::FinishVerdict;

    let first = command();
    let requesting = run_id();
    let rule = ResolutionRequester::Rule(FinishVerdict::granted());
    let completed = JobBuilder::new()
        .with_requester(Requester::Run(requesting))
        .with_resolution(JobResolution::completed(resolution_id(), ts(30)).remembering(Some(first)))
        .build();
    assert!(matches!(
        completed.repeat_of_finish(first, &rule),
        Ok(Some(JobEvent::JobCompleted(_)))
    ));
    let failed = JobBuilder::new()
        .with_requester(Requester::Run(requesting))
        .with_resolution(
            JobResolution::failed(
                resolution_id(),
                ts(30),
                JobFailureCause::DeclaredByOwner,
                None,
            )
            .unwrap()
            .remembering(Some(first)),
        )
        .build();
    assert_eq!(
        failed.repeat_of_fail(first, &rule),
        Err(JobsError::NotRequester)
    );
}
