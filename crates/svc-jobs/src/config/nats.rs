use super::env::declared;
use crate::error::ServiceError;

pub struct NatsCredentials {
    pub user: String,
    pub password: String,
}

pub(super) fn nats_credentials() -> Result<Option<NatsCredentials>, ServiceError> {
    match (declared("NATS_USER"), declared("NATS_PASSWORD")) {
        (None, None) => Ok(None),
        (Some(user), Some(password)) => Ok(Some(NatsCredentials { user, password })),
        (Some(_), None) => Err(ServiceError::MissingConfiguration {
            key: "NATS_PASSWORD",
        }),
        (None, Some(_)) => Err(ServiceError::MissingConfiguration { key: "NATS_USER" }),
    }
}
