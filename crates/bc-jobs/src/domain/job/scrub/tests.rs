use super::*;
use crate::domain::job::resolution::{JobFailureCause, JobResolution};
use crate::domain::run::failure::RunFailureKind;
use crate::fixtures::{JobBuilder, RunBuilder, job_id, resolution_id, ts};

fn failed() -> JobBuilder {
    JobBuilder::new()
        .with_run(
            RunBuilder::new(1)
                .started(ts(5))
                .failed(ts(20), RunFailureKind::Permanent)
                .build(),
        )
        .with_resolution(
            JobResolution::failed(
                resolution_id(),
                ts(30),
                JobFailureCause::DeclaredByOwner,
                None,
            )
            .unwrap(),
        )
}

fn completed() -> Job {
    JobBuilder::new()
        .with_run(RunBuilder::new(1).started(ts(5)).completed(ts(10)).build())
        .with_resolution(JobResolution::completed(resolution_id(), ts(30)))
        .build()
}

fn kinds(events: &[JobEvent]) -> Vec<(&'static str, JobId)> {
    events
        .iter()
        .map(|event| (event.event_type(), event.job_id()))
        .collect()
}

#[test]
fn every_job_of_the_tree_gets_the_erasure_fact_with_the_time_of_the_scrub() {
    let (first, second) = (completed(), completed());
    let events = events_of_a_scrub(&[first.clone(), second.clone()], ts(900));
    assert_eq!(
        kinds(&events),
        vec![
            ("JobContentErased", first.id()),
            ("JobContentErased", second.id())
        ]
    );
    for event in events {
        let JobEvent::JobContentErased(fact) = event else {
            panic!("only erasure facts here");
        };
        assert_eq!(fact.at, ts(900));
    }
}

#[test]
fn a_failed_job_that_could_be_retried_also_gets_the_affordance_fact() {
    let retriable = failed().build();
    let events = events_of_a_scrub(std::slice::from_ref(&retriable), ts(900));
    assert_eq!(
        kinds(&events),
        vec![
            ("JobContentErased", retriable.id()),
            ("JobAffordancesChanged", retriable.id())
        ]
    );
}

#[test]
fn a_failed_job_that_could_not_be_retried_before_gets_no_affordance_fact() {
    let already = failed().manually_retried_by(job_id(), false).build();
    let events = events_of_a_scrub(std::slice::from_ref(&already), ts(900));
    assert_eq!(kinds(&events), vec![("JobContentErased", already.id())]);
}

#[test]
fn a_job_already_erased_by_the_cancel_sweep_keeps_its_first_erasure_time() {
    let erased = JobBuilder::new()
        .with_resolution(JobResolution::cancelled(resolution_id(), ts(300)))
        .with_request_data_erased(ts(400))
        .build();
    let fresh = completed();
    let events = events_of_a_scrub(&[erased, fresh], ts(900));
    let times: Vec<_> = events
        .iter()
        .map(|event| match event {
            JobEvent::JobContentErased(fact) => fact.at,
            other => panic!("only erasure facts here, got {}", other.event_type()),
        })
        .collect();
    assert_eq!(times, vec![ts(400), ts(900)]);
}
