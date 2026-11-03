//! A pod still on 0.9.0 reads the facts this version writes: every KnownUser,
//! one known only by id included, is serialized in the shape it decodes.

use br_core_events::UserId;
use serde::Deserialize;
use serde_json::Value;
use uuid::Uuid;

use crate::domain::ids::KnownUserId;
use crate::domain::keys::ReasonCode;
use crate::domain::priority::Priority;
use crate::domain::references::KnownUser;
use crate::event::job::JobEvent;
use crate::event::job_facts::{
    JobDeleted, JobPriorityForced, ManualRetryStarted, RunCancellationRequested,
};
use crate::event::user::{UserEvent, UserMaxPriorityCleared, UserMaxPrioritySet};
use crate::fixtures::{job_id, manual_retry_id, resolution_id, run_id, user, user_passport};

/// A frozen copy of the KnownUser 0.9.0 decodes: a v7 id and a name that is
/// a non-blank string of at most 512 bytes once trimmed.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct KnownUserV090 {
    id: Uuid,
    display_name: String,
}

fn assert_read_by_0_9_0(payload: &Value, field: &str, user: &KnownUser) {
    let read: KnownUserV090 = serde_json::from_value(payload[field].clone())
        .unwrap_or_else(|error| panic!("0.9.0 reads `{field}` of {payload}: {error}"));
    assert_eq!(read.id, user.id().0);
    assert_eq!(read.id.get_version_num(), 7);
    let name = read.display_name.trim();
    assert!(!name.is_empty() && name.len() <= 512, "{payload}");
    assert_eq!(
        name,
        user.id().0.to_string(),
        "an id-only user carries its id"
    );
}

fn id_only() -> KnownUser {
    user().without_name()
}

fn queued_for(user: &KnownUser) -> JobEvent {
    use crate::commands::job::create::{CreateJob, CreateOutcome, create_job};
    let command = CreateJob {
        id: job_id(),
        runner_type: crate::fixtures::runner_type(),
        producer: crate::fixtures::producer(),
        config: None,
        parent_job_id: None,
        triggered_by: Some(user.clone()),
        source_entity_id: None,
        max_attempts: None,
        requested_priority: Priority::DEFAULT,
        initial_job_id: None,
        requester: crate::domain::requester::Requester::Service(None),
        passport: Some(user_passport(user)),
        payload: None,
        call: None,
    };
    let limits = crate::domain::policy::ServiceLimits::default();
    let outcome = create_job(command, None, None, None, None, &limits);
    match outcome {
        Ok(CreateOutcome::Queued(result)) => result.events.into_iter().next().unwrap(),
        other => panic!("a fresh declaration is queued, got {other:?}"),
    }
}

fn round_trip_job(event: &JobEvent) -> Value {
    let payload = event.payload().unwrap();
    let decoded = JobEvent::decode(event.event_type(), payload.clone()).unwrap();
    assert_eq!(&decoded, event, "this version reads back what it wrote");
    payload
}

fn round_trip_user(event: &UserEvent) -> Value {
    let payload = event.payload().unwrap();
    let decoded = UserEvent::decode(event.event_type(), payload.clone()).unwrap();
    assert_eq!(&decoded, event, "this version reads back what it wrote");
    payload
}

#[test]
fn a_job_queued_for_a_user_known_only_by_id_is_read_by_0_9_0() {
    let user = UserId(Uuid::now_v7());
    let known = KnownUser::id_only(user).unwrap();
    let payload = round_trip_job(&queued_for(&known));
    assert_read_by_0_9_0(&payload, "triggered_by", &known);
}

#[test]
fn the_job_facts_naming_a_user_known_only_by_id_are_read_by_0_9_0() {
    let known = id_only();
    let cancellation = JobEvent::RunCancellationRequested(RunCancellationRequested {
        job_id: job_id(),
        run_id: run_id(),
        reason_code: ReasonCode::new("job_cancelled").unwrap(),
        requested_by: Some(known.clone()),
        originating_job_id: None,
    });
    let deleted = JobEvent::JobDeleted(JobDeleted {
        job_id: job_id(),
        deleted_by: known.clone(),
    });
    let retried = JobEvent::ManualRetryStarted(ManualRetryStarted {
        job_id: job_id(),
        manual_retry_id: manual_retry_id(),
        failed_resolution_id: resolution_id(),
        successor_job_id: job_id(),
        requested_by: known.clone(),
        run_id: None,
    });
    let forced = JobEvent::JobPriorityForced(JobPriorityForced {
        job_id: job_id(),
        priority: Priority::new(5).unwrap(),
        forced_by: known.clone(),
        originating_job_id: None,
        requested_priority: None,
    });
    for (event, field) in [
        (cancellation, "requested_by"),
        (deleted, "deleted_by"),
        (retried, "requested_by"),
        (forced, "forced_by"),
    ] {
        assert_read_by_0_9_0(&round_trip_job(&event), field, &known);
    }
}

#[test]
fn the_maximum_priority_facts_naming_a_user_known_only_by_id_are_read_by_0_9_0() {
    let known = id_only();
    let subject = KnownUserId::new(Uuid::now_v7()).unwrap();
    let set = UserEvent::MaxPrioritySet(UserMaxPrioritySet {
        user_id: subject,
        max_priority: Priority::new(2).unwrap(),
        set_by: known.clone(),
    });
    let cleared = UserEvent::MaxPriorityCleared(UserMaxPriorityCleared {
        user_id: subject,
        cleared_by: known.clone(),
    });
    assert_read_by_0_9_0(&round_trip_user(&set), "set_by", &known);
    assert_read_by_0_9_0(&round_trip_user(&cleared), "cleared_by", &known);
}
