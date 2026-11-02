use super::*;
use crate::domain::job::JobCall;
use crate::domain::job::resolution::JobResolution;
use crate::domain::ownership::{ActorRef, DeclarationClaim};
use crate::domain::payload::CallMode;
use crate::domain::run::failure::RunFailureKind;
use crate::error::JobsError;
use crate::event::job::JobEvent;
use crate::fixtures::{JobBuilder, RunBuilder, resolution_id, run_id, ts};
use uuid::Uuid;

struct Scene {
    caller_run: RunId,
    facts: AutoFinishFacts,
}

fn call(mode: CallMode) -> JobCall {
    JobCall {
        mode,
        caller_address: match mode {
            CallMode::Background => Some(Uuid::now_v7()),
            CallMode::Sync => None,
        },
    }
}

fn called_job(caller_run: RunId, mode: CallMode, runs: Vec<crate::domain::run::Run>) -> Job {
    runs.into_iter()
        .fold(
            JobBuilder::new()
                .with_requester(Requester::Run(caller_run))
                .with_call(call(mode)),
            JobBuilder::with_run,
        )
        .build()
}

fn completed_run(id: RunId, dispatched: i64, completed: i64) -> crate::domain::run::Run {
    RunBuilder::new(1)
        .with_id(id)
        .dispatched_at(ts(dispatched))
        .started(ts(dispatched + 1))
        .completed(ts(completed))
        .build()
}

fn restart_job(
    caller_run: RunId,
    mode: CallMode,
    origin: RunId,
    runs: Vec<crate::domain::run::Run>,
) -> Job {
    runs.into_iter()
        .fold(
            JobBuilder::new()
                .with_requester(Requester::Run(caller_run))
                .with_call(call(mode))
                .with_restart_of(origin),
            JobBuilder::with_run,
        )
        .build()
}

fn caller_ended(run: RunId, kind: RunTerminalKind, at: i64) -> CallerRun {
    CallerRun {
        run_id: run,
        ended: Some(CallerRunEnd { kind, at: ts(at) }),
    }
}

fn answer(run: RunId, recorded: i64, read: Option<i64>) -> AnswerRecord {
    AnswerRecord {
        run_id: run,
        recorded_at: ts(recorded),
        read_at: read.map(ts),
    }
}

fn plain(mode: CallMode) -> (Scene, RunId) {
    let caller_run = run_id();
    let called_run = run_id();
    let scene = Scene {
        caller_run,
        facts: AutoFinishFacts {
            called: called_job(caller_run, mode, vec![completed_run(called_run, 10, 20)]),
            restart_jobs: vec![],
            restarts_due: vec![],
            answers: vec![answer(called_run, 20, Some(30))],
            caller_runs: vec![caller_ended(caller_run, RunTerminalKind::Completed, 40)],
        },
    };
    (scene, called_run)
}

fn verdict(scene: &Scene) -> Result<FinishByRule, AutoFinishWait> {
    scene.facts.decide()
}

#[test]
fn a_sync_answer_that_was_marked_before_the_caller_run_ended_with_success_finishes_the_job() {
    let (scene, _) = plain(CallMode::Sync);
    let plan = verdict(&scene).unwrap();
    assert_eq!(plan.called(), scene.facts.called.id());
    assert!(plan.restart_jobs_to_finish().is_empty());
}

#[test]
fn a_background_answer_that_was_read_before_the_caller_run_ended_with_success_finishes_the_job() {
    let (scene, _) = plain(CallMode::Background);
    assert!(verdict(&scene).is_ok());
}

#[test]
fn an_unread_answer_finishes_nothing_in_either_mode() {
    for mode in [CallMode::Sync, CallMode::Background] {
        let (mut scene, called_run) = plain(mode);
        scene.facts.answers = vec![answer(called_run, 20, None)];
        assert_eq!(verdict(&scene), Err(AutoFinishWait::AnswerNotRead));
    }
}

#[test]
fn a_background_job_whose_runner_sent_no_mail_has_no_answer() {
    let (mut scene, _) = plain(CallMode::Background);
    scene.facts.answers = vec![];
    assert_eq!(verdict(&scene), Err(AutoFinishWait::AnswerNotRead));
}

#[test]
fn a_caller_run_that_has_not_ended_finishes_nothing() {
    let (mut scene, _) = plain(CallMode::Sync);
    scene.facts.caller_runs = vec![CallerRun {
        run_id: scene.caller_run,
        ended: None,
    }];
    assert_eq!(verdict(&scene), Err(AutoFinishWait::CallerRunNotEnded));
}

#[test]
fn a_failed_or_cancelled_caller_finishes_nothing() {
    for kind in [RunTerminalKind::Failed, RunTerminalKind::Cancelled] {
        let (mut scene, _) = plain(CallMode::Background);
        scene.facts.caller_runs = vec![caller_ended(scene.caller_run, kind, 40)];
        assert_eq!(verdict(&scene), Err(AutoFinishWait::CallerDidNotSucceed));
    }
}

#[test]
fn the_first_run_of_a_caller_that_ended_before_the_answer_was_read_finishes_nothing() {
    let (mut scene, called_run) = plain(CallMode::Background);
    scene.facts.answers = vec![answer(called_run, 20, Some(50))];
    scene.facts.caller_runs = vec![caller_ended(
        scene.caller_run,
        RunTerminalKind::Completed,
        40,
    )];
    assert_eq!(verdict(&scene), Err(AutoFinishWait::CallerRunNotEnded));
}

#[test]
fn the_run_of_a_restart_job_of_the_caller_that_read_the_answer_finishes_the_job_when_it_ends() {
    let (mut scene, called_run) = plain(CallMode::Background);
    let restart_run = run_id();
    scene.facts.answers = vec![answer(called_run, 20, Some(50))];
    scene.facts.caller_runs = vec![
        caller_ended(scene.caller_run, RunTerminalKind::Completed, 40),
        CallerRun {
            run_id: restart_run,
            ended: None,
        },
    ];
    assert_eq!(verdict(&scene), Err(AutoFinishWait::CallerRunNotEnded));
    scene.facts.caller_runs[1] = caller_ended(restart_run, RunTerminalKind::Completed, 60);
    assert!(verdict(&scene).is_ok());
}

#[test]
fn a_restart_run_of_the_caller_that_failed_after_reading_the_answer_finishes_nothing() {
    let (mut scene, called_run) = plain(CallMode::Background);
    let restart_run = run_id();
    scene.facts.answers = vec![answer(called_run, 20, Some(50))];
    scene.facts.caller_runs = vec![
        caller_ended(scene.caller_run, RunTerminalKind::Completed, 40),
        caller_ended(restart_run, RunTerminalKind::Failed, 60),
    ];
    assert_eq!(verdict(&scene), Err(AutoFinishWait::CallerRunNotEnded));
}

#[test]
fn a_latest_run_that_is_live_or_failed_finishes_nothing() {
    let caller_run = run_id();
    let live = RunBuilder::new(1).started(ts(11)).build();
    let failed = RunBuilder::new(1)
        .started(ts(11))
        .failed(ts(15), RunFailureKind::Permanent)
        .build();
    for run in [live, failed] {
        let mut scene = plain(CallMode::Sync).0;
        scene.facts.called = called_job(caller_run, CallMode::Sync, vec![run]);
        scene.facts.caller_runs = vec![caller_ended(caller_run, RunTerminalKind::Completed, 40)];
        assert_eq!(verdict(&scene), Err(AutoFinishWait::LatestRunNotCompleted));
    }
}

#[test]
fn a_job_that_a_service_created_and_a_settled_job_are_never_finished_by_rule() {
    let (scene, _) = plain(CallMode::Sync);
    let mut by_service = scene.facts.clone();
    by_service.called = JobBuilder::new()
        .with_run(completed_run(run_id(), 10, 20))
        .build();
    assert_eq!(by_service.decide(), Err(AutoFinishWait::NotCalledByARun));
    let mut settled = scene.facts.clone();
    settled.called = JobBuilder::new()
        .with_requester(Requester::Run(scene.caller_run))
        .with_call(call(CallMode::Sync))
        .with_resolution(JobResolution::cancelled(resolution_id(), ts(25)))
        .build();
    assert_eq!(settled.decide(), Err(AutoFinishWait::AlreadySettled));
}

mod finishing;
mod restarts;
