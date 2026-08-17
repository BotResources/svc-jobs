mod support;

use bc_jobs::commands::fleet::{ObserveLoss, ObservePresence, observe_loss, observe_presence};
use bc_jobs::commands::job::run_progress::RunStartedFact;
use bc_jobs::domain::fleet::status::ReportedStatus;
use bc_jobs::domain::fleet::{RunnerType, RunnerTypeState};
use bc_jobs::domain::ids::{EventId, PresenceSessionId};
use bc_jobs::domain::keys::{InstanceKey, ReasonCode, RunnerTypeKey, RunnerVersion};
use bc_jobs::domain::run::parts::RunnerInstanceReference;
use bc_jobs::event::fleet::FleetEvent;
use bc_jobs::policies::fleet::{LostPresenceSession, runs_lost_with_instance};
use bc_jobs::ports::environment::IdFactory;
use bc_jobs::ports::fleet::FleetReader;
use bc_jobs::ports::job::JobReader;
use chrono::Utc;
use svc_jobs::ServiceError;
use svc_jobs::app::environment::{SystemClock, UuidV7Factory};
use svc_jobs::app::presence::{self, ObservedLoss};
use svc_jobs::app::write::{self, FleetChange, JobChange};
use svc_jobs::db::{PgStore, apply};

use support::contention::{
    Fixture, a_job_with_one_dispatched_run, a_registered_instance, a_second_live_instance,
    announcing, commit, ids, metadata, one_run, reloaded,
};

fn expired() -> ReasonCode {
    ReasonCode::new(bc_jobs::commands::fleet::PRESENCE_EXPIRED).expect("a valid reason code")
}

fn instance_a() -> InstanceKey {
    InstanceKey::new("instance-a").expect("a valid instance key")
}

#[tokio::test]
async fn two_instances_replaying_the_same_presence_change_write_one_fleet_fact() {
    // Given: a live runner instance, and the fleet state both instances hydrated before deciding
    let fixture = Fixture::start().await;
    let runner_type = "reporter";
    let key = RunnerTypeKey::new(runner_type).expect("a valid runner type");
    let decided_on = a_registered_instance(&fixture, runner_type).await;
    let reported = |status: ReportedStatus| ObservePresence {
        reported_status: status,
        ..announcing(decided_on.id(), &key, "instance-a")
    };

    // When: the KV watch hands the same status change to both instances at once
    let first = observe_presence(Some(&decided_on), reported(ReportedStatus::Draining))
        .expect("the first instance decides the change");
    let second = observe_presence(Some(&decided_on), reported(ReportedStatus::Draining))
        .expect("the second instance decides the same change");
    assert!(matches!(
        first.events.as_slice(),
        [FleetEvent::InstanceStatusReported(_)]
    ));
    let left_metadata = metadata();
    let right_metadata = metadata();
    let factory = ids();
    let (left, right) = tokio::join!(
        write::commit_fleet_events(
            &fixture.store,
            &factory,
            FleetChange {
                runner_type_id: decided_on.id(),
                runner_type: &key,
                decided_on: Some(&decided_on),
                events: &first.events,
            },
            &left_metadata,
            Utc::now(),
        ),
        write::commit_fleet_events(
            &fixture.store,
            &factory,
            FleetChange {
                runner_type_id: decided_on.id(),
                runner_type: &key,
                decided_on: Some(&decided_on),
                events: &second.events,
            },
            &right_metadata,
            Utc::now(),
        ),
    );

    // Then: exactly one instance recorded the change — the other reclaims nothing
    let outcomes = [left, right];
    let refused: Vec<&ServiceError> = outcomes
        .iter()
        .filter_map(|outcome| outcome.as_ref().err())
        .collect();
    assert_eq!(
        refused.len(),
        1,
        "a broadcast presence change must be recorded by one instance only, got {outcomes:?}",
    );
    assert!(
        matches!(refused[0], ServiceError::Contended),
        "the loser learns the fleet moved under it: {:?}",
        refused[0],
    );
    assert_eq!(
        fixture
            .count(
                "SELECT count(*) AS count FROM domain_events WHERE event_type = $1",
                "InstanceStatusReported",
            )
            .await,
        1,
        "a broadcast presence change never doubles the fleet history",
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn only_one_instance_records_a_presence_loss_so_orphaned_runs_are_reclaimed_once() {
    // Given: a live runner instance, seen by both service instances before either reacts
    let fixture = Fixture::start().await;
    let runner_type = "collector";
    let key = RunnerTypeKey::new(runner_type).expect("a valid runner type");
    let decided_on = a_registered_instance(&fixture, runner_type).await;
    let session = decided_on
        .instance(&instance_a())
        .expect("the instance is live")
        .session_id();
    let loss = || ObserveLoss {
        instance_key: instance_a(),
        session_id: session,
        reason_code: expired(),
    };

    // When: the KV watch hands the same expiry to both instances at once
    let first = observe_loss(&decided_on, loss()).expect("the first instance decides the loss");
    let second = observe_loss(&decided_on, loss()).expect("the second instance decides the loss");
    let left_metadata = metadata();
    let right_metadata = metadata();
    let factory = ids();
    let (left, right) = tokio::join!(
        write::commit_fleet_events(
            &fixture.store,
            &factory,
            FleetChange {
                runner_type_id: decided_on.id(),
                runner_type: &key,
                decided_on: Some(&decided_on),
                events: &first.events,
            },
            &left_metadata,
            Utc::now(),
        ),
        write::commit_fleet_events(
            &fixture.store,
            &factory,
            FleetChange {
                runner_type_id: decided_on.id(),
                runner_type: &key,
                decided_on: Some(&decided_on),
                events: &second.events,
            },
            &right_metadata,
            Utc::now(),
        ),
    );

    // Then: only one instance holds the loss, so run reclamation runs once and not twice
    let outcomes = [left, right];
    let refused: Vec<&ServiceError> = outcomes
        .iter()
        .filter_map(|outcome| outcome.as_ref().err())
        .collect();
    assert_eq!(
        refused.len(),
        1,
        "a disconnection carries no unique key of its own, so only the guard can settle the \
         race: {outcomes:?}",
    );
    assert!(matches!(refused[0], ServiceError::Contended));
    assert_eq!(
        fixture
            .count(
                "SELECT count(*) AS count FROM domain_events WHERE event_type = $1",
                "InstanceDisconnected",
            )
            .await,
        1,
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn an_instance_loss_is_recorded_even_when_the_rest_of_the_fleet_moves_under_it() {
    // Given: two live instances of one runner type
    let fixture = Fixture::start().await;
    let runner_type = "haulier";
    let key = RunnerTypeKey::new(runner_type).expect("a valid runner type");
    let first = a_registered_instance(&fixture, runner_type).await;
    let fleet = a_second_live_instance(&fixture, &first, "instance-b").await;
    let instance_b = InstanceKey::new("instance-b").expect("a valid instance key");

    // Given: another pod holds the runner type, about to report a status change of instance-a
    let mut concurrent = fixture
        .pool
        .begin()
        .await
        .expect("the concurrent transaction opens");
    PgStore::lock_runner_type(&mut concurrent, fleet.id(), &key)
        .await
        .expect("the concurrent pod holds the fleet");

    // When: this pod reacts to instance-b's expiry and reaches the write behind that holder
    let store = fixture.store.clone();
    let contended_key = key.clone();
    let contended_instance = instance_b.clone();
    let losing = tokio::spawn(async move {
        presence::record_loss(
            &store,
            &UuidV7Factory,
            &SystemClock,
            ObservedLoss {
                runner_type: &contended_key,
                instance_key: &contended_instance,
                reason_code: &expired(),
                session_id: None,
            },
        )
        .await
    });
    fixture.await_a_pod_blocked_on_the_fleet_lock().await;

    // When: the other pod's unrelated status change lands first, moving the whole fleet
    let reported = observe_presence(
        Some(&fleet),
        ObservePresence {
            reported_status: ReportedStatus::Draining,
            ..announcing(fleet.id(), &key, "instance-a")
        },
    )
    .expect("the other pod decides the status change");
    apply::apply_fleet_events(
        &mut concurrent,
        fleet.id(),
        &recorded(&reported.events),
        &metadata(),
        Utc::now(),
    )
    .await
    .expect("the concurrent status change is written");
    concurrent
        .commit()
        .await
        .expect("the concurrent pod commits");

    // Then: the loss is not dropped — a moved fleet makes it re-decide, never abandon
    let outcome = losing.await.expect("the loss task finishes");
    assert!(
        matches!(outcome, Ok(Some(_))),
        "a disconnection is delivered once and never replayed, so a fleet that moved under the \
         decision must be re-read, not treated as somebody else's write: {outcome:?}",
    );
    assert_eq!(
        fixture
            .count(
                "SELECT count(*) AS count FROM domain_events WHERE event_type = $1",
                "InstanceDisconnected",
            )
            .await,
        1,
        "the re-decided loss is recorded exactly once",
    );
    assert!(
        fixture.fleet_event_version("InstanceDisconnected").await
            > fixture.fleet_event_version("InstanceStatusReported").await,
        "the loss must be filed after the concurrent status change, or the two writes never \
         interleaved and this scenario proved nothing",
    );
    let settled = reloaded(&fixture, &key).await;
    assert!(
        settled.instance(&instance_b).is_none(),
        "the lost instance leaves the live fleet, so it stops electing work it can never run",
    );
    assert!(
        settled.instance(&instance_a()).is_some(),
        "the surviving instance keeps its own status change",
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_loss_racing_a_reconnection_never_closes_the_session_that_replaced_it() {
    // Given: a live instance executing a run
    let fixture = Fixture::start().await;
    let runner_type = "smelter";
    let key = RunnerTypeKey::new(runner_type).expect("a valid runner type");
    let fleet = a_registered_instance(&fixture, runner_type).await;
    let lost_session = fleet
        .instance(&instance_a())
        .expect("the instance is live")
        .session_id();
    let (job, run_id) = a_job_with_one_dispatched_run(&fixture, runner_type).await;
    let started = job
        .record_run_started(RunStartedFact {
            run_id,
            instance: RunnerInstanceReference::new(key.clone(), instance_a()),
        })
        .expect("the run start is decided");
    commit(
        &fixture,
        vec![JobChange::new(job.id(), Some(job.clone()), started.events)],
    )
    .await
    .expect("the run is executing on the instance about to be lost");

    // Given: another pod holds the runner type, about to record that very disconnection
    let mut concurrent = fixture
        .pool
        .begin()
        .await
        .expect("the concurrent transaction opens");
    PgStore::lock_runner_type(&mut concurrent, fleet.id(), &key)
        .await
        .expect("the concurrent pod holds the fleet");

    // When: this pod reacts to the expiry of the session it just read, and blocks behind that pod
    let store = fixture.store.clone();
    let contended_key = key.clone();
    let losing = tokio::spawn(async move {
        presence::record_loss(
            &store,
            &UuidV7Factory,
            &SystemClock,
            ObservedLoss {
                runner_type: &contended_key,
                instance_key: &instance_a(),
                reason_code: &expired(),
                session_id: None,
            },
        )
        .await
    });
    fixture.await_a_pod_blocked_on_the_fleet_lock().await;

    // When: the other pod records that same loss and the process comes back under a new session
    let disconnected = observe_loss(
        &fleet,
        ObserveLoss {
            instance_key: instance_a(),
            session_id: lost_session,
            reason_code: expired(),
        },
    )
    .expect("the other pod decides the loss it observed");
    let emptied = RunnerType::hydrate(RunnerTypeState {
        id: fleet.id(),
        key: key.clone(),
        registered_at: fleet.registered_at(),
        instances: vec![],
    })
    .expect("a runner type with no live instance loads");
    let reconnection = ObservePresence {
        session_id: PresenceSessionId::new(ids().next()).expect("a v7 session id"),
        version: RunnerVersion::new("1.4.3").expect("a valid version"),
        capacity: one_run(),
        ..announcing(fleet.id(), &key, "instance-a")
    };
    let new_session = reconnection.session_id;
    let reconnected =
        observe_presence(Some(&emptied), reconnection).expect("the process announces itself again");
    let mut moved = recorded(&disconnected.events);
    moved.extend(recorded(&reconnected.events));
    apply::apply_fleet_events(&mut concurrent, fleet.id(), &moved, &metadata(), Utc::now())
        .await
        .expect("the concurrent disconnection and reconnection are written");
    concurrent
        .commit()
        .await
        .expect("the concurrent pod commits");

    // Then: the blocked loss re-reads the fleet, finds a session it never observed, and stops
    let outcome = losing.await.expect("the loss task finishes");
    assert!(
        matches!(outcome, Ok(None)),
        "a loss observed on one session may not be re-decided against the session that replaced \
         it: the process is alive again, and the caller must not reclaim its runs: {outcome:?}",
    );
    assert_eq!(
        fixture
            .count(
                "SELECT count(*) AS count FROM domain_events WHERE event_type = $1",
                "InstanceDisconnected",
            )
            .await,
        1,
        "only the session that was actually lost is closed",
    );
    let settled = reloaded(&fixture, &key).await;
    assert_eq!(
        settled
            .instance(&instance_a())
            .expect("the reconnected instance is live")
            .session_id(),
        new_session,
        "the fresh session survives the stale loss untouched",
    );
    let carrying = JobReader::load(&fixture.store, job.id())
        .await
        .expect("the job loads")
        .expect("the job exists");
    assert!(
        !carrying
            .find_run(run_id)
            .expect("the run is still there")
            .is_terminal(),
        "the run the reconnected process is still executing must not be reclaimed under a loss \
         that belonged to its previous session",
    );
    fixture.shutdown().await;
}

fn recorded(events: &[FleetEvent]) -> Vec<(EventId, FleetEvent)> {
    events
        .iter()
        .map(|event| {
            (
                EventId::new(ids().next()).expect("a v7 event id"),
                event.clone(),
            )
        })
        .collect()
}

#[tokio::test]
async fn a_reclaim_takes_only_the_runs_the_lost_session_carried() {
    // Given: an instance executing a run under the session that is about to be lost
    let fixture = Fixture::start().await;
    let runner_type = "salvager";
    let key = RunnerTypeKey::new(runner_type).expect("a valid runner type");
    let fleet = a_registered_instance(&fixture, runner_type).await;
    let lost_session = fleet
        .instance(&instance_a())
        .expect("the instance is live")
        .session_id();
    let (job, carried_run) = a_job_with_one_dispatched_run(&fixture, runner_type).await;
    let started = job
        .record_run_started(RunStartedFact {
            run_id: carried_run,
            instance: RunnerInstanceReference::new(key.clone(), instance_a()),
        })
        .expect("the run start is decided");
    commit(
        &fixture,
        vec![JobChange::new(job.id(), Some(job.clone()), started.events)],
    )
    .await
    .expect("the run is executing on the session about to be lost");

    // When: that session is closed and the same process comes back under a new one, taking work
    let disconnected = observe_loss(
        &fleet,
        ObserveLoss {
            instance_key: instance_a(),
            session_id: lost_session,
            reason_code: expired(),
        },
    )
    .expect("the loss of the session is decided");
    write::commit_fleet_events(
        &fixture.store,
        &ids(),
        FleetChange {
            runner_type_id: fleet.id(),
            runner_type: &key,
            decided_on: Some(&fleet),
            events: &disconnected.events,
        },
        &metadata(),
        Utc::now(),
    )
    .await
    .expect("the loss is recorded");
    let emptied = RunnerType::hydrate(RunnerTypeState {
        id: fleet.id(),
        key: key.clone(),
        registered_at: fleet.registered_at(),
        instances: vec![],
    })
    .expect("a runner type with no live instance loads");
    let reconnected = observe_presence(Some(&emptied), announcing(fleet.id(), &key, "instance-a"))
        .expect("the process announces itself again");
    write::commit_fleet_events(
        &fixture.store,
        &ids(),
        FleetChange {
            runner_type_id: fleet.id(),
            runner_type: &key,
            decided_on: Some(&emptied),
            events: &reconnected.events,
        },
        &metadata(),
        Utc::now(),
    )
    .await
    .expect("the replacement session opens");
    let (successor, successor_run) = a_job_with_one_dispatched_run(&fixture, runner_type).await;
    let taken = successor
        .record_run_started(RunStartedFact {
            run_id: successor_run,
            instance: RunnerInstanceReference::new(key.clone(), instance_a()),
        })
        .expect("the replacement session claims a run");
    commit(
        &fixture,
        vec![JobChange::new(
            successor.id(),
            Some(successor.clone()),
            taken.events,
        )],
    )
    .await
    .expect("the replacement session is executing a run of its own");

    // Then: the reclaim of the closed session names its own run, and leaves the successor's alone
    let window = FleetReader::closed_presence_session(&fixture.store, lost_session)
        .await
        .expect("the closed session reads back")
        .expect("the session this pod closed is closed");
    let active = fixture
        .store
        .active_jobs_of_type(runner_type)
        .await
        .expect("the active jobs of the type load");
    let lost = runs_lost_with_instance(
        &LostPresenceSession::new(
            RunnerInstanceReference::new(key.clone(), instance_a()),
            window.connected_at,
            window.disconnected_at,
        ),
        &active,
    );
    assert_eq!(
        lost.iter().map(|run| run.run_id).collect::<Vec<_>>(),
        vec![carried_run],
        "an instance key outlives its sessions: a reclaim that took every run the key ever \
         carried would fail the run the replacement session is executing right now — {lost:?}",
    );
    fixture.shutdown().await;
}
