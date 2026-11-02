use super::*;
use crate::commands::job::cancellation::CancelJob;
use crate::commands::job::resolution::{FailJob, FinishJob};
use crate::domain::ids::CommandId;
use crate::domain::job::resolution::JobFailureCause;
use crate::domain::ownership::{ActorRef, DeclarationClaim};
use crate::domain::requester::{ActsFor, Caller, Requester};
use crate::fixtures::{JobBuilder, RunBuilder, resolution_id, run_id, ts};
use uuid::Uuid;

fn command() -> CommandId {
    CommandId::new(Uuid::now_v7())
}

fn declarant() -> ResolutionRequester {
    ResolutionRequester::Declarant
}

fn completed_by(command_id: CommandId) -> Job {
    JobBuilder::new()
        .with_resolution(
            JobResolution::completed(resolution_id(), ts(30)).remembering(Some(command_id)),
        )
        .build()
}

fn failed_by(command_id: CommandId) -> Job {
    JobBuilder::new()
        .with_resolution(
            JobResolution::failed(
                resolution_id(),
                ts(30),
                JobFailureCause::DeclaredByOwner,
                None,
            )
            .unwrap()
            .remembering(Some(command_id)),
        )
        .build()
}

fn cancelled_by(command_id: CommandId) -> Job {
    JobBuilder::new()
        .with_resolution(
            JobResolution::cancelled(resolution_id(), ts(30)).remembering(Some(command_id)),
        )
        .build()
}

#[test]
fn a_finish_repeated_under_its_command_id_gets_the_completion_again() {
    let first = command();
    let job = completed_by(first);
    let stored = job.resolution().unwrap().id();
    let repeat = job.repeat_of_finish(first, &declarant()).unwrap();
    assert_eq!(
        repeat,
        Some(JobEvent::JobCompleted(JobCompleted {
            job_id: job.id(),
            resolution_id: stored
        }))
    );
}

#[test]
fn a_fail_repeated_under_its_command_id_gets_the_failure_again_with_the_stored_cause() {
    let first = command();
    let job = failed_by(first);
    let stored = job.resolution().unwrap().id();
    let repeat = job.repeat_of_fail(first, &declarant()).unwrap();
    assert_eq!(
        repeat,
        Some(JobEvent::JobFailed(JobFailed {
            job_id: job.id(),
            resolution_id: stored,
            failure_cause: JobFailureCause::DeclaredByOwner,
            caused_by_run_id: None,
            report: None,
        }))
    );
}

#[test]
fn a_cancel_repeated_under_its_command_id_gets_the_cancellation_again() {
    let first = command();
    let job = cancelled_by(first);
    let repeat = job
        .repeat_of_cancel(first, &CancelRequester::Declarant)
        .unwrap();
    assert!(matches!(repeat, Some(JobEvent::JobCancelled(_))));
}

#[test]
fn a_declarant_repeating_a_cancel_of_a_terminal_job_is_still_answered() {
    let first = command();
    let job = cancelled_by(first);
    assert_eq!(
        job.clone().cancel(CancelJob {
            resolution_id: resolution_id(),
            requester: CancelRequester::Declarant
        }),
        Err(JobsError::JobAlreadyTerminal {
            status: "CANCELLED"
        })
    );
    assert!(
        job.repeat_of_cancel(first, &CancelRequester::Declarant)
            .unwrap()
            .is_some()
    );
}

#[test]
fn an_owner_repeating_a_cancel_of_a_terminal_job_is_still_answered() {
    let first = command();
    let job = cancelled_by(first);
    let owner = ActorRef::new(Uuid::now_v7());
    let claim = CancelRequester::Owner(DeclarationClaim::new(Some(owner), owner));
    assert_eq!(
        job.cancel(CancelJob {
            resolution_id: resolution_id(),
            requester: claim.clone(),
        }),
        Err(JobsError::JobAlreadyTerminal {
            status: "CANCELLED"
        })
    );
    assert!(job.repeat_of_cancel(first, &claim).unwrap().is_some());
}

#[test]
fn an_owner_that_did_not_declare_the_job_never_gets_the_first_cancel_outcome() {
    let first = command();
    let job = cancelled_by(first);
    let stranger = CancelRequester::Owner(DeclarationClaim::new(
        Some(ActorRef::new(Uuid::now_v7())),
        ActorRef::new(Uuid::now_v7()),
    ));
    assert_eq!(
        job.repeat_of_cancel(first, &stranger),
        Err(JobsError::NotOwner)
    );
}

#[test]
fn a_command_id_that_resolved_nothing_is_no_repeat() {
    let job = completed_by(command());
    assert_eq!(job.repeat_of_finish(command(), &declarant()), Ok(None));
    assert_eq!(job.repeat_of_fail(command(), &declarant()), Ok(None));
    assert_eq!(
        job.repeat_of_cancel(command(), &CancelRequester::Declarant),
        Ok(None)
    );
}

#[test]
fn a_job_that_is_not_resolved_has_nothing_to_repeat() {
    let job = JobBuilder::new().build();
    assert_eq!(job.repeat_of_finish(command(), &declarant()), Ok(None));
}

#[test]
fn a_resolution_without_a_remembered_command_is_no_repeat_of_any() {
    let job = JobBuilder::new()
        .with_resolution(JobResolution::completed(resolution_id(), ts(30)))
        .build();
    assert_eq!(job.repeat_of_finish(command(), &declarant()), Ok(None));
}

#[test]
fn the_command_id_of_a_finish_does_not_repeat_a_fail_or_a_cancel() {
    let first = command();
    let job = completed_by(first);
    assert_eq!(job.repeat_of_fail(first, &declarant()), Ok(None));
    assert_eq!(
        job.repeat_of_cancel(first, &CancelRequester::Declarant),
        Ok(None)
    );
}

#[test]
fn an_actor_that_may_not_finish_the_job_never_gets_the_first_outcome() {
    let first = command();
    let job = JobBuilder::new()
        .with_requester(Requester::Run(run_id()))
        .with_resolution(JobResolution::completed(resolution_id(), ts(30)).remembering(Some(first)))
        .build();
    assert_eq!(
        job.repeat_of_finish(first, &declarant()),
        Err(JobsError::NotRequester)
    );
}

#[test]
fn an_owner_that_did_not_declare_the_job_never_gets_the_first_outcome() {
    let first = command();
    let job = failed_by(first);
    let stranger = ResolutionRequester::LegacyOwner(DeclarationClaim::new(
        Some(ActorRef::new(Uuid::now_v7())),
        ActorRef::new(Uuid::now_v7()),
    ));
    assert_eq!(
        job.repeat_of_fail(first, &stranger),
        Err(JobsError::NotOwner)
    );
}

#[test]
fn a_run_that_is_not_the_requester_never_gets_the_first_cancel_outcome() {
    let first = command();
    let job = JobBuilder::new()
        .with_requester(Requester::Run(run_id()))
        .with_run(RunBuilder::new(1).started(ts(5)).build())
        .with_resolution(JobResolution::cancelled(resolution_id(), ts(30)).remembering(Some(first)))
        .build();
    let stranger = CancelRequester::Caller {
        caller: Caller::Run {
            run_id: run_id(),
            works_the_job: false,
        },
        acts_for: ActsFor::default(),
    };
    assert_eq!(
        job.repeat_of_cancel(first, &stranger),
        Err(JobsError::RunnerNotWorkingTheJob)
    );
}

#[test]
fn a_new_command_on_a_terminal_job_is_still_refused_by_the_ordinary_guards() {
    let job = completed_by(command());
    let result = job.finish(FinishJob {
        resolution_id: resolution_id(),
        requester: declarant(),
    });
    assert_eq!(
        result,
        Err(JobsError::JobAlreadyTerminal {
            status: "COMPLETED"
        })
    );
    let failing = job.declare_failed(FailJob {
        resolution_id: resolution_id(),
        requester: declarant(),
    });
    assert!(failing.is_err());
}

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
        caller: caller.clone(),
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
