use super::*;
use crate::domain::keys::DisplayName;
use crate::fixtures::{human_passport, passport_of};
use br_core_auth::{AuthMethod, PassportClaims};
use br_core_events::UserId;
use serde_json::json;

fn known(id: Uuid, name: &str) -> KnownUser {
    KnownUser::new(UserId(id), DisplayName::new(name).unwrap()).unwrap()
}

fn user_passport(id: Uuid) -> JobPassport {
    passport_of(human_passport(id))
}

#[test]
fn a_document_that_is_not_a_passport_is_refused() {
    for document in [
        json!([]),
        json!({ "kind": "user" }),
        json!({ "kind": "human", "user_id": Uuid::now_v7() }),
    ] {
        assert_eq!(
            JobPassport::from_document(document),
            Err(JobsError::PassportMalformed)
        );
    }
}

#[test]
fn the_subject_is_read_from_the_document() {
    let user = Uuid::now_v7();
    let service = Uuid::now_v7();
    assert_eq!(user_passport(user).subject(), PassportSubject::User(user));
    assert_eq!(user_passport(user).user_id(), Some(user));
    let for_service = passport_of(Passport::service(service, PassportClaims::new()));
    assert_eq!(for_service.subject(), PassportSubject::Service(service));
    assert_eq!(for_service.user_id(), None);
}

#[test]
fn the_known_user_of_the_passports_user_is_accepted_whatever_its_name() {
    let id = Uuid::now_v7();
    let passport = user_passport(id);
    assert_eq!(
        passport.guard_matches_user(Some(&known(id, "Alice (trial)"))),
        Ok(())
    );
    assert_eq!(passport.guard_matches_user(Some(&known(id, "Ada"))), Ok(()));
    assert_eq!(passport.guard_matches_user(None), Ok(()));
}

#[test]
fn a_known_user_other_than_the_passports_user_is_refused() {
    let passport = user_passport(Uuid::now_v7());
    let other = known(Uuid::now_v7(), "Ada");
    assert_eq!(
        passport.guard_matches_user(Some(&other)),
        Err(JobsError::PassportUserMismatch)
    );
}

#[test]
fn a_known_user_beside_a_service_passport_is_refused() {
    let passport = passport_of(Passport::service(Uuid::now_v7(), PassportClaims::new()));
    assert_eq!(
        passport.guard_matches_user(Some(&known(Uuid::now_v7(), "Ada"))),
        Err(JobsError::PassportUserMismatch)
    );
}

#[test]
fn the_same_document_is_the_passport_of_the_parent_and_another_is_not() {
    let id = Uuid::now_v7();
    let parent = user_passport(id);
    assert_eq!(
        user_passport(id).guard_is_the_passport_of_parent(Some(&parent)),
        Ok(())
    );
    assert_eq!(
        user_passport(id).guard_is_the_passport_of_parent(None),
        Ok(()),
        "a parent whose Passport was erased holds none to compare"
    );
    let inactive = passport_of(Passport::human(
        id,
        false,
        false,
        AuthMethod::Jwt,
        None,
        PassportClaims::new(),
    ));
    assert_eq!(
        inactive.guard_is_the_passport_of_parent(Some(&parent)),
        Err(JobsError::PassportParentMismatch)
    );
}

#[test]
fn a_passport_stored_with_a_display_name_reads_as_the_same_passport() {
    // Given: a Passport as 0.7.0 stored it, with the KnownUser's name copied in
    let id = Uuid::now_v7();
    let document = serde_json::to_value(human_passport(id)).unwrap();
    let stored = json!({
        "subject": { "User": id },
        "display_name": "Alice (trial)",
        "document": document,
    });
    let stored_unnamed = json!({
        "subject": { "User": id },
        "display_name": null,
        "document": document,
    });
    // When: it is read back
    let read: JobPassport = serde_json::from_value(stored).unwrap();
    let read_unnamed: JobPassport = serde_json::from_value(stored_unnamed).unwrap();
    // Then: it is the Passport of the same document, and is written without the name
    assert_eq!(read, user_passport(id));
    assert_eq!(read_unnamed, user_passport(id));
    assert_eq!(read.subject(), PassportSubject::User(id));
    assert_eq!(
        serde_json::to_value(&read).unwrap(),
        json!({ "subject": { "User": id }, "document": document })
    );
}
