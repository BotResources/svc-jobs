use super::*;

fn owner() -> ResolutionRequester {
    let actor = ActorRef::new(Uuid::now_v7());
    ResolutionRequester::LegacyOwner(DeclarationClaim::new(Some(actor), actor))
}

#[test]
fn the_finish_by_rule_is_a_plain_completion_that_asks_no_run_to_stop() {
    let (scene, _) = plain(CallMode::Sync);
    let plan = scene.facts.decide().unwrap();
    let resolution = resolution_id();
    let result = scene
        .facts
        .called
        .finish(plan.finish_command(resolution))
        .unwrap();
    match result.events.as_slice() {
        [JobEvent::JobCompleted(fact)] => {
            assert_eq!(fact.job_id, scene.facts.called.id());
            assert_eq!(fact.resolution_id, resolution);
        }
        other => panic!("expected exactly one completion, got {other:?}"),
    }
    assert!(result.warnings.is_empty());
}

#[test]
fn a_late_finish_by_the_library_after_the_rule_finished_the_job_is_refused() {
    let (scene, _) = plain(CallMode::Sync);
    let finished = JobBuilder::new()
        .with_requester(Requester::Run(scene.caller_run))
        .with_call(call(CallMode::Sync))
        .with_run(completed_run(run_id(), 10, 20))
        .with_resolution(JobResolution::completed(resolution_id(), ts(50)))
        .build();
    let late = finished.finish(FinishJob {
        resolution_id: resolution_id(),
        requester: owner(),
    });
    assert_eq!(
        late,
        Err(JobsError::JobAlreadyTerminal {
            status: "COMPLETED"
        })
    );
}

#[test]
fn a_finish_by_rule_is_not_a_repeat_of_any_command() {
    let (scene, _) = plain(CallMode::Sync);
    let plan = scene.facts.decide().unwrap();
    let finish = plan.finish_command(resolution_id());
    assert_eq!(
        scene
            .facts
            .called
            .repeat_of_finish(
                crate::domain::ids::CommandId::new(Uuid::now_v7()),
                &finish.requester
            )
            .unwrap(),
        None
    );
}

#[test]
fn a_finish_by_rule_cannot_reach_a_job_that_a_service_requested() {
    let (scene, _) = plain(CallMode::Sync);
    let plan = scene.facts.decide().unwrap();
    let by_service = JobBuilder::new()
        .with_run(completed_run(run_id(), 10, 20))
        .build();
    assert_eq!(
        by_service.finish(plan.finish_command(resolution_id())),
        Err(JobsError::NotRequester)
    );
}
