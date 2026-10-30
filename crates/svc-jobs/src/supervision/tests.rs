use std::sync::atomic::AtomicUsize;

use br_util_axum_readiness::Readiness;
use tokio::sync::Notify;

use super::*;

fn policy() -> RestartPolicy {
    RestartPolicy {
        initial_backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(1),
        stability: Duration::from_secs(3600),
    }
}

/// The shipped defaults: 250 ms doubling up to 30 s, reset after 60 s of uptime.
fn production_policy() -> RestartPolicy {
    RestartPolicy {
        initial_backoff: Duration::from_millis(250),
        max_backoff: Duration::from_secs(30),
        stability: Duration::from_secs(60),
    }
}

fn attempt_counter() -> Arc<AtomicUsize> {
    Arc::new(AtomicUsize::new(0))
}

fn spawn_binding_after(
    supervisor: &Supervisor,
    name: &'static str,
    failures: usize,
    tally: &Arc<AtomicUsize>,
) {
    let tally = tally.clone();
    supervisor.spawn(name, move |established| {
        let tally = tally.clone();
        async move {
            if tally.fetch_add(1, Ordering::SeqCst) < failures {
                drop(established);
                return Err(ServiceError::Infra("the bus is unreachable".to_owned()));
            }
            established.signal();
            std::future::pending::<()>().await;
            Ok(())
        }
    });
}

fn spawn_never_binding(supervisor: &Supervisor, name: &'static str, tally: &Arc<AtomicUsize>) {
    spawn_binding_after(supervisor, name, usize::MAX, tally);
}

fn spawn_failing_once(supervisor: &Supervisor, name: &'static str, tally: &Arc<AtomicUsize>) {
    let tally = tally.clone();
    supervisor.spawn(name, move |established| {
        let tally = tally.clone();
        async move {
            if tally.fetch_add(1, Ordering::SeqCst) == 0 {
                established.signal();
                return Err(ServiceError::Infra("the connection dropped".to_owned()));
            }
            established.signal();
            std::future::pending::<()>().await;
            Ok(())
        }
    });
}

fn spawn_failing_then_never_establishing(
    supervisor: &Supervisor,
    name: &'static str,
    tally: &Arc<AtomicUsize>,
) {
    let tally = tally.clone();
    supervisor.spawn(name, move |established| {
        let tally = tally.clone();
        async move {
            if tally.fetch_add(1, Ordering::SeqCst) == 0 {
                established.signal();
                return Err(ServiceError::Infra("the connection dropped".to_owned()));
            }
            drop(established);
            std::future::pending::<()>().await;
            Ok(())
        }
    });
}

fn spawn_healthy(supervisor: &Supervisor, name: &'static str, tally: &Arc<AtomicUsize>) {
    let tally = tally.clone();
    supervisor.spawn(name, move |established| {
        let tally = tally.clone();
        async move {
            tally.fetch_add(1, Ordering::SeqCst);
            established.signal();
            std::future::pending::<()>().await;
            Ok(())
        }
    });
}

fn spawn_establishing_when_released(
    supervisor: &Supervisor,
    name: &'static str,
    release: &Arc<Notify>,
) {
    let release = release.clone();
    supervisor.spawn(name, move |established| {
        let release = release.clone();
        async move {
            release.notified().await;
            established.signal();
            std::future::pending::<()>().await;
            Ok(())
        }
    });
}

async fn until(condition: impl Fn() -> bool) {
    for _ in 0..2000 {
        if condition() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("the supervised tasks never reached the state this scenario waits for");
}

#[tokio::test(start_paused = true)]
async fn a_task_that_has_not_bound_its_source_of_work_keeps_a_booted_pod_out_of_rotation() {
    // Given: a spawned task that has not yet bound the subscription it lives on
    let readiness = ReadinessHandle::ready();
    let supervisor = Supervisor::new(readiness.clone(), policy());
    let release = Arc::new(Notify::new());
    spawn_establishing_when_released(&supervisor, "presence_watch", &release);

    // When: boot completes while that task is still binding
    supervisor.boot_complete();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Then: spawned is not bound — the pod serves nothing and names what is missing
    assert_eq!(
        readiness.snapshot(),
        Readiness::NotReady {
            reason: "background tasks are down: presence_watch".to_owned()
        },
        "a subscription that is not bound yet can still miss the signal it exists to catch",
    );

    // When: the task binds its source of work
    release.notify_one();

    // Then: readiness opens on the establishment, not on the spawn
    until(|| readiness.is_ready()).await;
}

#[tokio::test(start_paused = true)]
async fn a_restarted_task_reopens_the_pod_only_when_it_binds_again() {
    // Given: a booted service whose task fails once, then runs without ever binding again
    let readiness = ReadinessHandle::ready();
    let supervisor = Supervisor::new(readiness.clone(), policy());
    supervisor.boot_complete();
    let attempts = attempt_counter();
    spawn_failing_then_never_establishing(&supervisor, "hub_pump", &attempts);

    // When: the retry runs and holds
    until(|| attempts.load(Ordering::SeqCst) == 2).await;
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Then: a running attempt is not a bound subscription — readiness stays down
    assert_eq!(
        readiness.snapshot(),
        Readiness::NotReady {
            reason: "background tasks are down: hub_pump".to_owned()
        },
        "restarting a task re-establishes nothing on its own; only the task can say it is bound",
    );
}

#[tokio::test(start_paused = true)]
async fn a_task_failing_far_past_the_old_budget_is_still_retried_and_reopens_the_pod_on_binding() {
    // Given: a booted service whose task cannot bind for 25 attempts in a row — far more than the
    // ten quick restarts 0.5.0 allowed before giving up — and a healthy task beside it
    let readiness = ReadinessHandle::ready();
    let supervisor = Supervisor::new(readiness.clone(), policy());
    supervisor.boot_complete();
    let attempts = attempt_counter();
    let healthy = attempt_counter();
    spawn_binding_after(&supervisor, "job-creation consumer", 25, &attempts);
    spawn_healthy(&supervisor, "dispatch loop", &healthy);

    // When: the bus is still out, a dozen attempts in
    until(|| attempts.load(Ordering::SeqCst) >= 12).await;

    // Then: the pod is out of rotation, and names the task that is down
    assert_eq!(
        readiness.snapshot(),
        Readiness::NotReady {
            reason: "background tasks are down: job-creation consumer".to_owned()
        },
        "a task that has not bound again keeps the pod out of rotation",
    );

    // When: the bus comes back and the 26th attempt binds
    until(|| attempts.load(Ordering::SeqCst) == 26).await;

    // Then: the pod is ready again on its own — no budget ran out, no operator restarted it
    until(|| readiness.is_ready()).await;
    assert_eq!(
        healthy.load(Ordering::SeqCst),
        1,
        "the healthy task never restarted"
    );
}

#[tokio::test(start_paused = true)]
async fn a_task_whose_bus_stays_out_for_an_hour_is_still_being_retried() {
    // Given: a booted service on the shipped restart policy, whose task never binds
    let readiness = ReadinessHandle::ready();
    let supervisor = Supervisor::new(readiness.clone(), production_policy());
    supervisor.boot_complete();
    let attempts = attempt_counter();
    spawn_never_binding(&supervisor, "runner status consumer", &attempts);

    // When: the outage lasts half an hour, then another half hour
    tokio::time::sleep(Duration::from_secs(30 * 60)).await;
    let after_half_an_hour = attempts.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_secs(30 * 60)).await;
    let after_an_hour = attempts.load(Ordering::SeqCst);

    // Then: the retries went past the ten the old budget allowed, and they never stop — at least
    // one attempt per maximum backoff, 30 s
    assert!(
        after_half_an_hour > 10,
        "{after_half_an_hour} attempts in half an hour"
    );
    assert!(
        after_an_hour >= after_half_an_hour + 50,
        "a parked task probes its bus at least every 30 s: {after_half_an_hour} then \
         {after_an_hour}",
    );

    // Then: meanwhile the pod is out of rotation, naming the task
    assert_eq!(
        readiness.snapshot(),
        Readiness::NotReady {
            reason: "background tasks are down: runner status consumer".to_owned()
        },
    );
}

#[tokio::test(start_paused = true)]
async fn readiness_returns_only_once_the_task_that_failed_is_running_again() {
    // Given: a booted service whose task fails once and then holds
    let readiness = ReadinessHandle::ready();
    let supervisor = Supervisor::new(readiness.clone(), policy());
    supervisor.boot_complete();
    let attempts = attempt_counter();
    spawn_failing_once(&supervisor, "hub_pump", &attempts);

    // When: the task is restarted
    until(|| attempts.load(Ordering::SeqCst) == 2).await;

    // Then: readiness comes back on its own, without an operator restart
    until(|| readiness.is_ready()).await;
}

#[tokio::test(start_paused = true)]
async fn a_restart_before_boot_completes_never_declares_the_pod_ready() {
    // Given: a supervisor whose service has not finished booting
    let readiness = ReadinessHandle::not_ready("booting");
    let supervisor = Supervisor::new(readiness.clone(), policy());
    let attempts = attempt_counter();
    spawn_failing_once(&supervisor, "hub_pump", &attempts);

    // When: a task dies and is restarted before boot ever completed
    until(|| attempts.load(Ordering::SeqCst) == 2).await;
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Then: a running task is not a booted service — readiness stays down
    assert!(
        !readiness.is_ready(),
        "only boot completion opens the pod to traffic; a restarted task never does",
    );
}

#[test]
fn a_declared_dependency_recovers_readiness_only_after_reconciliation_marks_it_up() {
    let readiness = ReadinessHandle::not_ready("booting");
    let supervisor = Supervisor::new(readiness.clone(), policy());
    supervisor.boot_complete();
    assert!(readiness.is_ready());

    supervisor.mark_down("runner type Published Language catalog");
    assert_eq!(
        readiness.snapshot(),
        Readiness::NotReady {
            reason: "background tasks are down: runner type Published Language catalog".to_owned(),
        }
    );

    supervisor.mark_up("runner type Published Language catalog");
    assert!(readiness.is_ready());
}
