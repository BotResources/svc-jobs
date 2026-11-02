use super::*;

struct Restarted {
    scene: Scene,
    called_run: RunId,
    origin: RunId,
}

fn restarted(mode: CallMode, restart_runs: Vec<crate::domain::run::Run>) -> Restarted {
    let (mut scene, called_run) = plain(mode);
    let caller_run = scene.caller_run;
    scene.facts.restart_jobs = vec![restart_job(caller_run, mode, called_run, restart_runs)];
    Restarted {
        scene,
        called_run,
        origin: called_run,
    }
}

fn live_run() -> crate::domain::run::Run {
    RunBuilder::new(1)
        .dispatched_at(ts(50))
        .started(ts(51))
        .build()
}

#[test]
fn a_restart_job_that_waits_for_dispatch_a_scheduled_retry_or_a_live_run_blocks_the_finish() {
    let waiting_for_a_retry = RunBuilder::new(1)
        .dispatched_at(ts(50))
        .started(ts(51))
        .failed(ts(55), RunFailureKind::Transient)
        .retry_due(ts(90))
        .build();
    for runs in [vec![], vec![waiting_for_a_retry], vec![live_run()]] {
        let case = restarted(CallMode::Background, runs);
        assert_eq!(
            case.scene.facts.decide(),
            Err(AutoFinishWait::RestartJobPending)
        );
    }
}

#[test]
fn a_restart_that_is_due_blocks_the_finish_before_any_restart_job_exists() {
    let (mut scene, _) = plain(CallMode::Background);
    scene.facts.restarts_due = vec![run_id()];
    assert_eq!(scene.facts.decide(), Err(AutoFinishWait::RestartDue));
}

#[test]
fn a_restart_job_that_failed_for_good_or_was_cancelled_alone_leaves_the_run_the_job_had_before() {
    let failed_run = RunBuilder::new(1)
        .dispatched_at(ts(50))
        .started(ts(51))
        .failed(ts(55), RunFailureKind::Permanent)
        .build();
    let cancelled_run = RunBuilder::new(1)
        .dispatched_at(ts(50))
        .started(ts(51))
        .cancelled(ts(55))
        .build();
    for (run, resolution) in [
        (
            failed_run,
            JobResolution::failed(
                resolution_id(),
                ts(55),
                crate::domain::job::resolution::JobFailureCause::DeclaredByOwner,
                None,
            )
            .unwrap(),
        ),
        (
            cancelled_run,
            JobResolution::cancelled(resolution_id(), ts(55)),
        ),
    ] {
        let mut case = restarted(CallMode::Background, vec![run]);
        let caller_run = case.scene.caller_run;
        case.scene.facts.restart_jobs = vec![
            JobBuilder::new()
                .with_requester(Requester::Run(caller_run))
                .with_call(call(CallMode::Background))
                .with_restart_of(case.origin)
                .with_run(case.scene.facts.restart_jobs[0].runs()[0].clone())
                .with_resolution(resolution)
                .build(),
        ];
        let plan = case.scene.facts.decide().unwrap();
        assert!(plan.restart_jobs_to_finish().is_empty());
        assert_eq!(plan.premise().restart_jobs.len(), 1);
    }
}

fn completed_restart_run() -> crate::domain::run::Run {
    completed_run(run_id(), 50, 70)
}

#[test]
fn a_restart_run_that_completed_with_a_read_answer_finishes_the_job_and_its_restart_job() {
    let restart_run = completed_restart_run();
    let mut case = restarted(CallMode::Background, vec![restart_run.clone()]);
    case.scene.facts.answers = vec![
        answer(case.called_run, 20, Some(30)),
        answer(restart_run.id(), 70, Some(80)),
    ];
    case.scene.facts.caller_runs = vec![
        caller_ended(case.scene.caller_run, RunTerminalKind::Completed, 40),
        caller_ended(run_id(), RunTerminalKind::Completed, 90),
    ];
    let plan = case.scene.facts.decide().unwrap();
    assert_eq!(
        plan.restart_jobs_to_finish(),
        [case.scene.facts.restart_jobs[0].id()]
    );
}

#[test]
fn a_restart_run_that_sent_no_mail_falls_back_on_the_last_earlier_mail_the_caller_read() {
    let mut case = restarted(CallMode::Background, vec![completed_restart_run()]);
    case.scene.facts.answers = vec![answer(case.called_run, 20, Some(30))];
    assert!(case.scene.facts.decide().is_ok());
    case.scene.facts.answers = vec![answer(case.called_run, 20, None)];
    assert_eq!(
        case.scene.facts.decide(),
        Err(AutoFinishWait::AnswerNotRead)
    );
    case.scene.facts.answers = vec![];
    assert_eq!(
        case.scene.facts.decide(),
        Err(AutoFinishWait::AnswerNotRead)
    );
}

#[test]
fn an_unread_mail_of_the_restart_run_is_not_replaced_by_an_earlier_read_mail() {
    let restart_run = completed_restart_run();
    let mut case = restarted(CallMode::Background, vec![restart_run.clone()]);
    case.scene.facts.answers = vec![
        answer(case.called_run, 20, Some(30)),
        answer(restart_run.id(), 70, None),
    ];
    assert_eq!(
        case.scene.facts.decide(),
        Err(AutoFinishWait::AnswerNotRead)
    );
}

#[test]
fn the_last_earlier_mail_is_the_one_that_counts() {
    let mut case = restarted(CallMode::Background, vec![completed_restart_run()]);
    let in_between = run_id();
    case.scene.facts.answers = vec![
        answer(case.called_run, 20, Some(30)),
        answer(in_between, 45, None),
    ];
    assert_eq!(
        case.scene.facts.decide(),
        Err(AutoFinishWait::AnswerNotRead)
    );
}

#[test]
fn a_sync_restart_run_does_not_wait_for_its_new_notice_when_the_earlier_one_was_marked() {
    let restart_run = completed_restart_run();
    let mut case = restarted(CallMode::Sync, vec![restart_run.clone()]);
    case.scene.facts.answers = vec![
        answer(case.called_run, 20, Some(30)),
        answer(restart_run.id(), 70, None),
    ];
    assert!(case.scene.facts.decide().is_ok());
    case.scene.facts.answers[0] = answer(case.called_run, 20, None);
    assert_eq!(
        case.scene.facts.decide(),
        Err(AutoFinishWait::AnswerNotRead)
    );
}

#[test]
fn a_sync_job_without_restart_never_borrows_the_notice_of_another_job() {
    let (mut scene, called_run) = plain(CallMode::Sync);
    scene.facts.answers = vec![answer(called_run, 20, None), answer(run_id(), 15, Some(16))];
    assert_eq!(scene.facts.decide(), Err(AutoFinishWait::AnswerNotRead));
}
