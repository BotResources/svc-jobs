use super::*;
use crate::domain::job::resolution::JobResolution;
use crate::domain::keys::InstanceKey;
use crate::domain::run::Run;
use crate::fixtures::{JobBuilder, RunBuilder, instance, resolution_id, runner_type, start_id, ts};

fn other_instance() -> RunnerInstanceReference {
    RunnerInstanceReference::new(runner_type(), InstanceKey::new("pod-9").unwrap())
}

fn of_another_type() -> RunnerInstanceReference {
    RunnerInstanceReference::new(
        crate::domain::keys::RunnerTypeKey::new("scribe").unwrap(),
        InstanceKey::new("pod-7").unwrap(),
    )
}

fn job_of(run: Run) -> Job {
    JobBuilder::new().with_run(run).build()
}

fn request(
    run: &Run,
    instance: RunnerInstanceReference,
    start_id: StartId,
    present: bool,
) -> StartRequestFact {
    StartRequestFact {
        run_id: run.id(),
        instance,
        start_id,
        instance_present: present,
    }
}

fn pending() -> Run {
    RunBuilder::new(1).build()
}

fn held_by_pod_7_under(start: StartId) -> Run {
    RunBuilder::new(1)
        .started_under(instance(), ts(5), start)
        .build()
}

fn held_by_the_status_fact() -> Run {
    RunBuilder::new(1).started(ts(5)).build()
}

fn reserved_for_pod_7() -> Run {
    RunBuilder::new(1).reserved(ts(1)).build()
}

fn refusal(run: Run, fact: impl Fn(&Run) -> StartRequestFact) -> StartRefusal {
    let fact = fact(&run);
    job_of(run).record_run_started_request(fact).unwrap_err()
}

fn holds(result: JobCommandResult, start: StartId) {
    match result.events.as_slice() {
        [JobEvent::RunStarted(fact)] => {
            assert_eq!(fact.start_id, Some(start));
            assert_eq!(fact.instance_key.as_str(), "pod-7");
        }
        other => panic!("expected the hold, got {other:?}"),
    }
}

#[test]
fn a_pending_run_is_held_by_the_instance_under_its_start_id() {
    let run = pending();
    let start = start_id();
    let result = job_of(run.clone())
        .record_run_started_request(request(&run, instance(), start, true))
        .unwrap();
    holds(result, start);
}

#[test]
fn the_same_start_id_from_the_holder_is_accepted_again_and_changes_nothing() {
    let start = start_id();
    let run = held_by_pod_7_under(start);
    let result = job_of(run.clone())
        .record_run_started_request(request(&run, instance(), start, true))
        .unwrap();
    assert!(result.is_empty());
    assert_eq!(
        result.warnings,
        vec![CommandWarning::FactAlreadyRecorded {
            fact: "RunStartedRequest"
        }]
    );
}

#[test]
fn the_holder_is_accepted_again_even_when_its_presence_is_gone() {
    let start = start_id();
    let run = held_by_pod_7_under(start);
    let result = job_of(run.clone())
        .record_run_started_request(request(&run, instance(), start, false))
        .unwrap();
    assert!(result.is_empty());
}

#[test]
fn another_start_id_from_the_holder_is_refused_as_held() {
    let run = held_by_pod_7_under(start_id());
    let refused = refusal(run, |run| request(run, instance(), start_id(), true));
    assert_eq!(refused, StartRefusal::RunHeld);
}

#[test]
fn another_instance_is_refused_as_held_with_the_start_id_of_the_holder_or_another() {
    let start = start_id();
    let run = held_by_pod_7_under(start);
    let same_id = refusal(run.clone(), |run| {
        request(run, other_instance(), start, true)
    });
    let other_id = refusal(run, |run| request(run, other_instance(), start_id(), true));
    assert_eq!(
        (same_id, other_id),
        (StartRefusal::RunHeld, StartRefusal::RunHeld)
    );
}

#[test]
fn a_run_held_by_the_status_stream_fact_refuses_every_start_id() {
    let from_the_holder = refusal(held_by_the_status_fact(), |run| {
        request(run, instance(), start_id(), true)
    });
    let from_another = refusal(held_by_the_status_fact(), |run| {
        request(run, other_instance(), start_id(), true)
    });
    assert_eq!(
        (from_the_holder, from_another),
        (StartRefusal::RunHeld, StartRefusal::RunHeld)
    );
}

#[test]
fn a_run_reserved_for_another_instance_is_refused_and_the_reserved_instance_is_held() {
    let refused = refusal(reserved_for_pod_7(), |run| {
        request(run, other_instance(), start_id(), true)
    });
    assert_eq!(refused, StartRefusal::RunReservedElsewhere);
    let run = reserved_for_pod_7();
    let start = start_id();
    let result = job_of(run.clone())
        .record_run_started_request(request(&run, instance(), start, true))
        .unwrap();
    holds(result, start);
}

#[test]
fn an_ended_run_is_refused_as_not_pending_also_for_the_start_id_that_held_it() {
    let start = start_id();
    let held_then_completed = RunBuilder::new(1)
        .started_under(instance(), ts(5), start)
        .completed(ts(9))
        .build();
    let repeat = refusal(held_then_completed, |run| {
        request(run, instance(), start, true)
    });
    let withdrawn_unstarted = RunBuilder::new(1).cancelled(ts(9)).build();
    let new_id = refusal(withdrawn_unstarted, |run| {
        request(run, instance(), start_id(), true)
    });
    let failed = RunBuilder::new(1)
        .started(ts(5))
        .failed(
            ts(9),
            crate::domain::run::failure::RunFailureKind::Transient,
        )
        .build();
    let after_failure = refusal(failed, |run| request(run, instance(), start_id(), true));
    let cancelled = RunBuilder::new(1).cancelled(ts(9)).build();
    let after_cancel = refusal(cancelled, |run| request(run, instance(), start_id(), true));
    assert_eq!(
        [repeat, new_id, after_failure, after_cancel],
        [StartRefusal::RunNotPending; 4]
    );
}

#[test]
fn a_run_of_a_terminal_job_is_refused_as_not_pending() {
    let run = pending();
    let job = JobBuilder::new()
        .with_run(run.clone())
        .with_resolution(JobResolution::cancelled(resolution_id(), ts(30)))
        .build();
    let refused = job
        .record_run_started_request(request(&run, instance(), start_id(), true))
        .unwrap_err();
    assert_eq!(refused, StartRefusal::RunNotPending);
}

#[test]
fn an_instance_of_another_runner_type_is_refused_as_an_unknown_run() {
    let refused = refusal(pending(), |run| {
        request(run, of_another_type(), start_id(), true)
    });
    assert_eq!(refused, StartRefusal::RunUnknown);
}

#[test]
fn a_run_the_job_never_dispatched_is_refused_as_unknown() {
    let job = job_of(pending());
    let stranger = StartRequestFact {
        run_id: crate::fixtures::run_id(),
        instance: instance(),
        start_id: start_id(),
        instance_present: true,
    };
    assert_eq!(
        job.record_run_started_request(stranger).unwrap_err(),
        StartRefusal::RunUnknown
    );
}

#[test]
fn an_instance_without_presence_is_refused_only_when_nothing_else_refuses_first() {
    let unknown_on_a_pending_run =
        refusal(pending(), |run| request(run, instance(), start_id(), false));
    let unknown_on_a_held_run = refusal(held_by_pod_7_under(start_id()), |run| {
        request(run, other_instance(), start_id(), false)
    });
    let unknown_on_an_ended_run = refusal(RunBuilder::new(1).cancelled(ts(9)).build(), |run| {
        request(run, instance(), start_id(), false)
    });
    let unknown_on_a_run_reserved_elsewhere = refusal(reserved_for_pod_7(), |run| {
        request(run, other_instance(), start_id(), false)
    });
    assert_eq!(
        [
            unknown_on_a_pending_run,
            unknown_on_a_held_run,
            unknown_on_an_ended_run,
            unknown_on_a_run_reserved_elsewhere
        ],
        [
            StartRefusal::InstanceUnknown,
            StartRefusal::RunHeld,
            StartRefusal::RunNotPending,
            StartRefusal::RunReservedElsewhere
        ]
    );
}

#[test]
fn the_texts_of_the_refusals_name_no_job_no_parent_no_initial_job_and_no_passport() {
    let all = [
        StartRefusal::RunUnknown,
        StartRefusal::InstanceUnknown,
        StartRefusal::RunHeld,
        StartRefusal::RunReservedElsewhere,
        StartRefusal::RunNotPending,
        StartRefusal::Malformed,
    ];
    for refusal in all {
        let words = refusal.message().to_lowercase().replace("jobs", "");
        for forbidden in ["job", "parent", "initial", "passport"] {
            assert!(
                !words.contains(forbidden),
                "{} names '{forbidden}': {words}",
                refusal.code()
            );
        }
        assert!(!refusal.code().is_empty());
    }
}

#[test]
fn the_codes_are_the_wire_reasons() {
    let codes: Vec<&str> = [
        StartRefusal::RunUnknown,
        StartRefusal::InstanceUnknown,
        StartRefusal::RunHeld,
        StartRefusal::RunReservedElsewhere,
        StartRefusal::RunNotPending,
        StartRefusal::Malformed,
    ]
    .iter()
    .map(StartRefusal::code)
    .collect();
    assert_eq!(
        codes,
        [
            "run_unknown",
            "instance_unknown",
            "run_held",
            "run_reserved_elsewhere",
            "run_not_pending",
            "malformed"
        ]
    );
}
