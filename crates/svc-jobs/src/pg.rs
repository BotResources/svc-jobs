//! The one PostgreSQL gesture that happens before any port exists: observing
//! whether the runtime role is already in its desired end state.

use std::str::FromStr;

use sqlx::postgres::PgConnectOptions;
use sqlx::{ConnectOptions, Connection};

use crate::error::ServiceError;

/// Three-valued probe: does `role` already authenticate with `password` against
/// the database in `database_url`? The same guard svc-identity, svc-projects,
/// svc-services, svc-tasks, svc-timesheet and svc-website carry, for the same
/// reason.
///
/// * `Ok(true)`  — the connection succeeds; the role is in its desired end
///   state, so the caller MUST skip `ensure_app_role` (a second-session `ALTER
///   ROLE … PASSWORD` fails `permission denied to alter role` under PG 16's
///   default `createrole_self_grant=''`: the implicit membership the owner
///   acquired by creating the role is revoked by the CNPG roles reconciler, so
///   every boot after the first is denied and the pod CrashLoops).
/// * `Ok(false)` — rejected with a credentials-class SQL state (`28P01` /
///   `28000` / `3D000`: role or database not provisioned yet). The expected
///   fresh-database shape; the caller falls through to `ensure_app_role`.
/// * `Err(_)`    — any other failure (TCP / TLS / URL). The catalog state could
///   not be observed; surface it loud instead of masking it with a
///   fall-through ALTER. Every such failure is labelled with [`probe_failed`],
///   because a bare `connection refused` in the boot error names neither the
///   probe nor the role.
///
/// **This guard covers restart, not rotation.** It answers whether the role
/// accepts the password the pod was configured with, so once that password
/// changes the probe reports `Ok(false)` and provisioning runs into the same
/// denied ALTER. Rotating a runtime role's password belongs to the CNPG roles
/// reconciler, which owns the role, and not to a service booting against it.
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

/// Name the gesture that failed. This runs before anything the service serves
/// exists, so the label is the whole of the operator's context: which role was
/// being probed, and that the failure is an observation rather than a write.
fn probe_failed(role: &str, cause: impl std::fmt::Display) -> ServiceError {
    ServiceError::Infra(format!(
        "probing {role} credentials against the runtime database: {cause}"
    ))
}

/// Whether a PostgreSQL SQLSTATE marks the "role or database not provisioned
/// yet" shape the probe answers `Ok(false)` to: `28P01` invalid_password,
/// `28000` invalid_authorization_specification, `3D000` invalid_catalog_name.
/// Any other state means the catalog could not be observed — surface it, never
/// mask it.
fn is_credentials_class(code: Option<&str>) -> bool {
    matches!(code, Some("28P01") | Some("28000") | Some("3D000"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The three states that mean "there is nothing to authenticate against
    /// yet" — the fresh-database shape, where provisioning must still run.
    #[test]
    fn the_credentials_class_is_the_not_yet_provisioned_shape() {
        assert!(is_credentials_class(Some("28P01")));
        assert!(is_credentials_class(Some("28000")));
        assert!(is_credentials_class(Some("3D000")));
    }

    /// Everything else means the catalog could not be observed. Falling through
    /// to `ensure_app_role` there would replace a legible failure with
    /// `permission denied to alter role` — which is the breach this guard
    /// exists to end.
    #[test]
    fn any_other_state_surfaces_rather_than_falling_through_to_the_alter() {
        assert!(!is_credentials_class(Some("42501"))); // insufficient_privilege
        assert!(!is_credentials_class(Some("53300"))); // too_many_connections
        assert!(!is_credentials_class(None));
    }

    /// The label is the operator's only context at this point in the boot.
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
