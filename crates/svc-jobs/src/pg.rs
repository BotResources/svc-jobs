use std::str::FromStr;

use sqlx::postgres::PgConnectOptions;
use sqlx::{ConnectOptions, Connection};

use crate::error::ServiceError;

pub(crate) async fn role_password_already_works(
    database_url: &str,
    role: &str,
    password: &str,
) -> Result<bool, ServiceError> {
    let options = PgConnectOptions::from_str(database_url)
        .map_err(|error| probe_failed(role, error))?
        .username(role)
        .password(password)
        .disable_statement_logging();
    match sqlx::PgConnection::connect_with(&options).await {
        Ok(connection) => {
            let _ = connection.close().await;
            Ok(true)
        }
        Err(sqlx::Error::Database(database_error)) => {
            let code = database_error.code();
            if is_credentials_class(code.as_deref()) {
                tracing::debug!(
                    role,
                    sqlstate = code.as_deref().unwrap_or("?"),
                    "runtime role probe: not yet provisioned"
                );
                Ok(false)
            } else {
                let sqlstate = code.as_deref().unwrap_or("?").to_owned();
                Err(probe_failed(
                    role,
                    format!("PostgreSQL answered SQLSTATE {sqlstate} — {database_error}"),
                ))
            }
        }
        Err(error) => Err(probe_failed(role, error)),
    }
}

fn probe_failed(role: &str, cause: impl std::fmt::Display) -> ServiceError {
    ServiceError::Infra(format!(
        "probing {role} credentials against the runtime database: {cause}"
    ))
}

fn is_credentials_class(code: Option<&str>) -> bool {
    matches!(code, Some("28P01") | Some("28000") | Some("3D000"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_credentials_class_is_the_not_yet_provisioned_shape() {
        assert!(is_credentials_class(Some("28P01")));
        assert!(is_credentials_class(Some("28000")));
        assert!(is_credentials_class(Some("3D000")));
    }

    #[test]
    fn any_other_state_surfaces_rather_than_falling_through_to_the_alter() {
        assert!(!is_credentials_class(Some("42501")));
        assert!(!is_credentials_class(Some("53300")));
        assert!(!is_credentials_class(None));
    }

    #[test]
    fn a_probe_failure_names_the_role_and_the_gesture() {
        let error = probe_failed("jobs_app", "connection refused");
        assert_eq!(
            error.to_string(),
            "infrastructure_failure: probing jobs_app credentials against the runtime database: \
             connection refused"
        );
    }
}
