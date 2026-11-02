use super::*;
use crate::fixtures::{RunBuilder, instance, start_id, ts};

#[test]
fn a_run_nobody_took_is_pending() {
    let run = RunBuilder::new(1).build();
    assert_eq!(run.start_standing(), StartStanding::Pending);
}

#[test]
fn a_claimed_run_is_reserved_for_its_instance() {
    let run = RunBuilder::new(1).reserved(ts(1)).build();
    assert_eq!(
        run.start_standing(),
        StartStanding::ReservedFor(&instance())
    );
}

#[test]
fn a_started_run_is_held_by_its_start() {
    let id = start_id();
    let run = RunBuilder::new(1)
        .started_under(instance(), ts(5), id)
        .build();
    let standing = run.start_standing();
    assert!(matches!(standing, StartStanding::HeldBy(start) if start.start_id() == Some(id)));
}

#[test]
fn a_reserved_run_that_started_is_held_not_reserved() {
    let run = RunBuilder::new(1).reserved(ts(1)).started(ts(5)).build();
    assert!(matches!(run.start_standing(), StartStanding::HeldBy(_)));
}

#[test]
fn a_run_that_ended_is_ended_whatever_held_it() {
    let started = RunBuilder::new(1).started(ts(5)).completed(ts(9)).build();
    let withdrawn = RunBuilder::new(1).cancelled(ts(9)).build();
    assert_eq!(started.start_standing(), StartStanding::Ended);
    assert_eq!(withdrawn.start_standing(), StartStanding::Ended);
}
