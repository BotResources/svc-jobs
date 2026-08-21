use std::sync::atomic::AtomicUsize;

use br_util_axum_readiness::Readiness;
use tokio::sync::Notify;

use super::*;

fn policy(budget: u32) -> RestartPolicy {
    RestartPolicy {
        initial_backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(1),
        budget,
        stability: Duration::from_secs(3600),
    }
}

fn attempt_counter() -> Arc<AtomicUsize> {
    Arc::new(AtomicUsize::new(0))
}

fn spawn_always_failing(supervisor: &Supervisor, name: &'static str, tally: &Arc<AtomicUsize>) {
    let tally = tally.clone();
    supervisor.spawn(name, move |established| {
        let tally = tally.clone();
        async move {
            tally.fetch_add(1, Ordering::SeqCst);
            established.signal();
            Err(ServiceError::Infra("the source of work died".to_owned()))
        }
    });
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
    let supervisor = Supervisor::new(readiness.clone(), policy(8));
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
    let supervisor = Supervisor::new(readiness.clone(), policy(8));
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
async fn a_task_that_exhausts_its_restart_budget_leaves_the_pod_not_ready_and_names_it() {
    // Given: a supervisor whose booted service runs one doomed task and one healthy task
    let readiness = ReadinessHandle::ready();
    let supervisor = Supervisor::new(readiness.clone(), policy(1));
    supervisor.boot_complete();
    let doomed = attempt_counter();
    let healthy = attempt_counter();
    spawn_always_failing(&supervisor, "presence_watch", &doomed);
    spawn_healthy(&supervisor, "trigger_consumer", &healthy);

    // When: the doomed task burns its budget
    until(|| doomed.load(Ordering::SeqCst) == 2 && healthy.load(Ordering::SeqCst) == 1).await;

    // Then: the pod stays out of rotation, and says which task is missing
    assert_eq!(
        readiness.snapshot(),
        Readiness::NotReady {
            reason: "background tasks are down: presence_watch".to_owned()
        },
        "a pod that lost a background task serves nothing and names the loss",
    );

    // Then: the loop is over — a burnt budget never retries in silence
    tokio::time::sleep(Duration::from_secs(60)).await;
    assert_eq!(doomed.load(Ordering::SeqCst), 2);
}

#[tokio::test(start_paused = true)]
async fn readiness_returns_only_once_the_task_that_failed_is_running_again() {
    // Given: a booted service whose task fails once and then holds
    let readiness = ReadinessHandle::ready();
    let supervisor = Supervisor::new(readiness.clone(), policy(8));
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
    let supervisor = Supervisor::new(readiness.clone(), policy(8));
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
fn the_restart_backoff_doubles_up_to_its_ceiling_and_never_overflows() {
    // Given: a policy backing off from 100ms up to 30s
    let policy = RestartPolicy {
        initial_backoff: Duration::from_millis(100),
        max_backoff: Duration::from_secs(30),
        budget: 64,
        stability: Duration::from_secs(60),
    };

    // When/Then: each consecutive restart doubles the wait, then flattens at the ceiling
    assert_eq!(policy.backoff(0), Duration::from_millis(100));
    assert_eq!(policy.backoff(1), Duration::from_millis(200));
    assert_eq!(policy.backoff(3), Duration::from_millis(800));
    assert_eq!(policy.backoff(8), Duration::from_millis(25_600));
    assert_eq!(policy.backoff(9), Duration::from_secs(30));

    // Then: a long-crashing task shifts no further than the clamp, whatever the count
    assert_eq!(policy.backoff(u32::MAX), Duration::from_secs(30));
}

#[test]
fn a_declared_dependency_recovers_readiness_only_after_reconciliation_marks_it_up() {
    let readiness = ReadinessHandle::not_ready("booting");
    let supervisor = Supervisor::new(readiness.clone(), policy(8));
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
