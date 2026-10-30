use std::sync::{Arc, Mutex};

use super::*;

fn failing(reason: &'static str) -> Pending<'static> {
    Box::pin(async move { Err(ServiceError::Infra(reason.to_owned())) })
}

fn succeeding() -> Pending<'static> {
    Box::pin(async { Ok(()) })
}

fn recorded(
    ran: &Arc<Mutex<Vec<Backstop>>>,
    backstop: Backstop,
    step: Pending<'static>,
) -> Pending<'static> {
    let ran = Arc::clone(ran);
    Box::pin(async move {
        ran.lock().unwrap().push(backstop);
        step.await
    })
}

#[test]
fn the_restart_wakes_run_after_every_safety_backstop() {
    let position = |wanted: Backstop| {
        Backstop::SWEEP
            .iter()
            .position(|backstop| *backstop == wanted)
            .expect("the backstop is in the sweep")
    };
    for backstop in Backstop::SWEEP {
        if backstop != Backstop::DueRestarts {
            assert!(
                position(backstop) < position(Backstop::DueRestarts),
                "{backstop:?} runs before the restart wakes"
            );
        }
    }
}

#[test]
fn no_backstop_runs_twice_in_the_sweep() {
    for backstop in Backstop::SWEEP {
        let occurrences = Backstop::SWEEP
            .iter()
            .filter(|other| **other == backstop)
            .count();
        assert_eq!(occurrences, 1, "{backstop:?}");
    }
}

#[tokio::test]
async fn a_failing_safety_backstop_does_not_stop_the_backstops_after_it() {
    let ran = Arc::new(Mutex::new(Vec::new()));

    let outcome = run_in_order(&Backstop::SWEEP, |backstop| {
        let step = match backstop {
            Backstop::AccessPaths => failing("the access paths"),
            _ => succeeding(),
        };
        recorded(&ran, backstop, step)
    })
    .await;

    assert_eq!(*ran.lock().unwrap(), Backstop::SWEEP.to_vec());
    assert!(matches!(outcome, Err(ServiceError::Infra(reason)) if reason == "the access paths"));
}

#[tokio::test]
async fn the_restart_wakes_run_although_every_safety_backstop_failed() {
    let ran = Arc::new(Mutex::new(Vec::new()));

    let outcome = run_in_order(&Backstop::SWEEP, |backstop| {
        let step = match backstop {
            Backstop::DueRestarts => succeeding(),
            _ => failing("a safety backstop"),
        };
        recorded(&ran, backstop, step)
    })
    .await;

    assert_eq!(ran.lock().unwrap().last(), Some(&Backstop::DueRestarts));
    assert!(outcome.is_err());
}

#[tokio::test]
async fn the_sweep_reports_the_first_failure_and_no_later_one() {
    let outcome = run_in_order(&Backstop::SWEEP, |backstop| match backstop {
        Backstop::LostClaims => failing("first"),
        Backstop::InactiveJobs => failing("second"),
        _ => succeeding(),
    })
    .await;

    assert!(matches!(outcome, Err(ServiceError::Infra(reason)) if reason == "first"));
}

#[tokio::test]
async fn a_sweep_without_failures_succeeds() {
    assert!(
        run_in_order(&Backstop::SWEEP, |_| succeeding())
            .await
            .is_ok()
    );
}
