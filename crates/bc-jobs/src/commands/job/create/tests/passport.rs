use super::*;
use crate::domain::job::JobCall;
use crate::domain::keys::DisplayName;
use crate::domain::payload::CallMode;
use crate::fixtures::human_passport;
use br_core_auth::{AuthMethod, Passport, PassportClaims};
use br_core_events::UserId;
use serde_json::Value;
use uuid::Uuid;

fn alice_trial() -> KnownUser {
    KnownUser::new(
        UserId(Uuid::now_v7()),
        DisplayName::new("Alice (trial)").unwrap(),
    )
    .unwrap()
}

fn document_of(user: &KnownUser) -> Value {
    serde_json::to_value(human_passport(user.id().0)).unwrap()
}

/// The same user's Passport with other claims: another document.
fn other_document_of(user: &KnownUser) -> Value {
    let mut claims = serde_json::Map::new();
    claims.insert("scopes".to_owned(), json!(["admin:everything"]));
    serde_json::to_value(Passport::human(
        user.id().0,
        false,
        true,
        AuthMethod::Jwt,
        None,
        PassportClaims::from_map(claims),
    ))
    .unwrap()
}

fn passport_from(document: Value) -> JobPassport {
    JobPassport::from_document(document).unwrap()
}

/// A root Job its producer created for `user`, naming it, whose Run calls.
fn root_of(user: &KnownUser) -> Job {
    JobBuilder::new()
        .with_triggered_by(user.clone())
        .with_passport(passport_from(document_of(user)))
        .with_run(RunBuilder::new(1).build())
        .build()
}

/// A sync call from the Run of `parent`, carrying `passport` and `triggered_by`.
fn sync_call_under(
    parent: &Job,
    passport: JobPassport,
    triggered_by: Option<KnownUser>,
) -> CreateJob {
    CreateJob {
        parent_job_id: Some(parent.id()),
        requester: Requester::Run(crate::fixtures::run_id()),
        call: Some(JobCall {
            mode: CallMode::Sync,
            caller_address: None,
        }),
        triggered_by,
        passport: Some(passport),
        ..command()
    }
}

fn create_under(parent: &Job, command: CreateJob) -> Result<CreateOutcome, JobsError> {
    create_job(command, None, Some(parent), None, &ServiceLimits::default())
}

#[test]
fn a_call_with_the_passport_of_its_parent_and_no_known_user_is_queued_for_the_parents_user() {
    // Given: a root Job created for Alice, named "Alice (trial)" by its producer
    let alice = alice_trial();
    let parent = root_of(&alice);
    // When: its Run calls with the same Passport document and names no KnownUser
    let child = sync_call_under(&parent, passport_from(document_of(&alice)), None);
    let fact = queued(create_under(&parent, child).unwrap());
    // Then: the child is queued with that Passport, for Alice, stating no name of hers
    assert_eq!(fact.parent_job_id, Some(parent.id()));
    assert_eq!(fact.passport.as_ref(), parent.passport());
    assert_eq!(fact.triggered_by, Some(alice.without_name()));
}

#[test]
fn a_call_with_the_passport_of_its_parent_and_another_name_for_its_user_is_queued() {
    // Given: a root Job created for Alice, named "Alice (trial)"
    let alice = alice_trial();
    let parent = root_of(&alice);
    // When: its Run calls with the same Passport and names Alice differently
    let renamed = KnownUser::new(alice.id(), DisplayName::new("Alice").unwrap()).unwrap();
    let child = sync_call_under(
        &parent,
        passport_from(document_of(&alice)),
        Some(renamed.clone()),
    );
    let fact = queued(create_under(&parent, child).unwrap());
    // Then: the name takes no part in the Passport check; the child keeps the name it was given
    assert_eq!(fact.passport.as_ref(), parent.passport());
    assert_eq!(fact.triggered_by, Some(renamed));
}

#[test]
fn a_call_with_another_passport_document_than_its_parent_is_refused() {
    // Given: a root Job created for Alice
    let alice = alice_trial();
    let parent = root_of(&alice);
    // When: its Run calls with a Passport of the same user but another document
    let child = sync_call_under(&parent, passport_from(other_document_of(&alice)), None);
    // Then: the Passport is not the one of the parent
    assert_eq!(
        create_under(&parent, child),
        Err(JobsError::PassportParentMismatch)
    );
}

#[test]
fn a_call_with_the_passport_of_another_user_is_refused() {
    // Given: a root Job created for Alice
    let alice = alice_trial();
    let parent = root_of(&alice);
    // When: its Run calls with the Passport of another user
    let bob = user();
    let child = sync_call_under(&parent, passport_from(document_of(&bob)), None);
    // Then: the Passport is not the one of the parent
    assert_eq!(
        create_under(&parent, child),
        Err(JobsError::PassportParentMismatch)
    );
}

#[test]
fn a_call_naming_a_known_user_who_is_not_the_user_of_its_passport_is_refused() {
    // Given: a root Job created for Alice
    let alice = alice_trial();
    let parent = root_of(&alice);
    // When: its Run calls with Alice's Passport but names another KnownUser
    let child = sync_call_under(&parent, passport_from(document_of(&alice)), Some(user()));
    // Then: the KnownUser is not the user of the Passport
    assert_eq!(
        create_under(&parent, child),
        Err(JobsError::PassportUserMismatch)
    );
}

#[test]
fn a_root_naming_a_known_user_who_is_not_the_user_of_its_passport_is_refused() {
    // Given: no parent
    let alice = alice_trial();
    // When: a producer creates a root Job with Alice's Passport and names another user
    let command = CreateJob {
        triggered_by: Some(user()),
        passport: Some(passport_from(document_of(&alice))),
        ..command()
    };
    let result = create_job(command, None, None, None, &ServiceLimits::default());
    // Then: the KnownUser is not the user of the Passport
    assert_eq!(result, Err(JobsError::PassportUserMismatch));
}

#[test]
fn a_root_naming_no_known_user_runs_for_the_user_of_its_passport_known_by_id() {
    // Given: no parent and no name for the user
    let alice = alice_trial();
    // When: a producer creates a root Job with Alice's Passport and names no KnownUser
    let command = CreateJob {
        passport: Some(passport_from(document_of(&alice))),
        ..command()
    };
    let fact = queued(create_job(command, None, None, None, &ServiceLimits::default()).unwrap());
    // Then: the KnownUser is Alice, known only by her id
    assert_eq!(
        fact.triggered_by,
        Some(KnownUser::id_only(alice.id()).unwrap())
    );
}

#[test]
fn a_call_without_a_passport_takes_the_passport_and_the_known_user_of_its_parent() {
    // Given: a root Job created for Alice, named "Alice (trial)"
    let alice = alice_trial();
    let parent = root_of(&alice);
    // When: its Run calls without a Passport and without a KnownUser
    let child = CreateJob {
        passport: None,
        ..sync_call_under(&parent, passport_from(document_of(&alice)), None)
    };
    let fact = queued(create_under(&parent, child).unwrap());
    // Then: the child holds the parent's Passport and runs for Alice, stating no name of hers
    assert_eq!(fact.passport.as_ref(), parent.passport());
    assert_eq!(fact.triggered_by, Some(alice.without_name()));
}

#[test]
fn a_redelivered_call_with_the_passport_of_its_parent_is_absorbed() {
    // Given: a call already queued with the Passport of its parent
    let alice = alice_trial();
    let parent = root_of(&alice);
    let child = sync_call_under(&parent, passport_from(document_of(&alice)), None);
    let fact = queued(create_under(&parent, child.clone()).unwrap());
    let existing = JobBuilder::new()
        .with_id(child.id)
        .with_parent(parent.id())
        .with_requester(child.requester)
        .with_call(child.call.unwrap())
        .with_triggered_by(fact.triggered_by.unwrap())
        .with_passport(fact.passport.unwrap())
        .build();
    // When: the same call is delivered again
    let outcome = create_job(
        child,
        Some(&existing),
        Some(&parent),
        None,
        &ServiceLimits::default(),
    );
    // Then: it is absorbed
    assert_eq!(outcome, Ok(CreateOutcome::AlreadyQueued));
}
