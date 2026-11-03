use br_core_auth::Passport;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::domain::references::KnownUser;
use crate::error::JobsError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PassportSubject {
    User(Uuid),
    Service(Uuid),
}

/// The identity a Job runs for: a Passport document, handed unchanged to the
/// runner, and the subject read from it. A Passport is its document: two
/// Passports are equal when their documents are, and it holds no display
/// name, which belongs to the KnownUser (`triggered_by`).
///
/// The subject is stored beside the document, so a stored Passport loads
/// without parsing its document again. A Passport stored before 0.10.0 also
/// carries a `display_name` copied from the KnownUser of its creation; it is
/// ignored when read.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobPassport {
    subject: PassportSubject,
    document: Value,
}

impl PartialEq for JobPassport {
    fn eq(&self, other: &Self) -> bool {
        self.document == other.document
    }
}

impl Eq for JobPassport {}

impl JobPassport {
    /// Reads a Passport document. One that is not a Passport is refused
    /// `PassportMalformed`.
    pub fn from_document(document: Value) -> Result<Self, JobsError> {
        let parsed: Passport =
            serde_json::from_value(document.clone()).map_err(|_| JobsError::PassportMalformed)?;
        let subject = match parsed {
            Passport::Human { user_id, .. } => PassportSubject::User(user_id),
            Passport::Service {
                service_account_id, ..
            } => PassportSubject::Service(service_account_id),
        };
        Ok(Self { subject, document })
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

    pub fn guard_matches_user(&self, triggered_by: Option<&KnownUser>) -> Result<(), JobsError> {
        match (self.user_id(), triggered_by) {
            (_, None) => Ok(()),
            (Some(user), Some(known)) if known.id().0 == user => Ok(()),
            _ => Err(JobsError::PassportUserMismatch),
        }
    }

    pub fn guard_is_the_passport_of_parent(
        &self,
        parent: Option<&JobPassport>,
    ) -> Result<(), JobsError> {
        match parent {
            Some(stored) if stored != self => Err(JobsError::PassportParentMismatch),
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests;
