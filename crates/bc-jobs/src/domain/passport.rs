use br_core_events::UserId;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::domain::keys::DisplayName;
use crate::domain::references::KnownUser;
use crate::error::JobsError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PassportSubject {
    User(Uuid),
    Service(Uuid),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobPassport {
    subject: PassportSubject,
    display_name: Option<String>,
    document: Value,
}

impl JobPassport {
    pub fn new(
        subject: PassportSubject,
        display_name: Option<String>,
        document: Value,
    ) -> Result<Self, JobsError> {
        if !document.is_object() {
            return Err(JobsError::NotAJsonObject { field: "passport" });
        }
        Ok(Self {
            subject,
            display_name,
            document,
        })
    }

    pub fn subject(&self) -> PassportSubject {
        self.subject
    }

    pub fn document(&self) -> &Value {
        &self.document
    }

    pub fn user_id(&self) -> Option<Uuid> {
        match self.subject {
            PassportSubject::User(id) => Some(id),
            PassportSubject::Service(_) => None,
        }
    }

    pub fn known_user(&self) -> Result<Option<KnownUser>, JobsError> {
        let Some(id) = self.user_id() else {
            return Ok(None);
        };
        let name = self
            .display_name
            .clone()
            .filter(|name| !name.trim().is_empty())
            .unwrap_or_else(|| id.to_string());
        KnownUser::new(UserId(id), DisplayName::new(name)?).map(Some)
    }

    pub fn guard_matches_user(&self, triggered_by: Option<&KnownUser>) -> Result<(), JobsError> {
        match (self.user_id(), triggered_by) {
            (_, None) => Ok(()),
            (Some(user), Some(known)) if known.id().0 == user => Ok(()),
            _ => Err(JobsError::PassportUserMismatch),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn user_passport(id: Uuid) -> JobPassport {
        JobPassport::new(
            PassportSubject::User(id),
            Some("Ada".to_owned()),
            json!({ "kind": "human" }),
        )
        .unwrap()
    }

    #[test]
    fn a_passport_document_must_be_an_object() {
        let result = JobPassport::new(PassportSubject::Service(Uuid::now_v7()), None, json!([]));
        assert_eq!(result, Err(JobsError::NotAJsonObject { field: "passport" }));
    }

    #[test]
    fn a_users_passport_names_the_known_user_it_runs_for() {
        let id = Uuid::now_v7();
        let known = user_passport(id).known_user().unwrap().unwrap();
        assert_eq!(known.id(), UserId(id));
        assert_eq!(known.display_name().as_str(), "Ada");
    }

    #[test]
    fn a_service_passport_runs_for_no_user() {
        let passport =
            JobPassport::new(PassportSubject::Service(Uuid::now_v7()), None, json!({})).unwrap();
        assert_eq!(passport.known_user().unwrap(), None);
    }

    #[test]
    fn a_known_user_other_than_the_passports_user_is_refused() {
        let passport = user_passport(Uuid::now_v7());
        let other = user_passport(Uuid::now_v7()).known_user().unwrap().unwrap();
        assert_eq!(
            passport.guard_matches_user(Some(&other)),
            Err(JobsError::PassportUserMismatch)
        );
    }

    #[test]
    fn a_known_user_beside_a_service_passport_is_refused() {
        let passport =
            JobPassport::new(PassportSubject::Service(Uuid::now_v7()), None, json!({})).unwrap();
        let known = user_passport(Uuid::now_v7()).known_user().unwrap().unwrap();
        assert_eq!(
            passport.guard_matches_user(Some(&known)),
            Err(JobsError::PassportUserMismatch)
        );
    }
}
