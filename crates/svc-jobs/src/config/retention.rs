use bc_jobs::JobsError;
use bc_jobs::domain::policy::{
    DEFAULT_CANCELLED_RETENTION_DAYS, DEFAULT_RETENTION_DAYS, Retention,
};

use super::env::parse;
use crate::error::ServiceError;

const RETENTION_KEY: &str = "JOBS_RETENTION_DAYS";
const CANCELLED_RETENTION_KEY: &str = "JOBS_CANCELLED_RETENTION_DAYS";

pub(super) fn retention_from_environment() -> Result<Retention, ServiceError> {
    retention_from(
        std::env::var(RETENTION_KEY).ok().as_deref(),
        std::env::var(CANCELLED_RETENTION_KEY).ok().as_deref(),
    )
}

pub(super) fn retention_from(
    retention_days: Option<&str>,
    cancelled_days: Option<&str>,
) -> Result<Retention, ServiceError> {
    let retention_days: i64 = parse(RETENTION_KEY, retention_days, DEFAULT_RETENTION_DAYS)?;
    let cancelled_days: i64 = parse(
        CANCELLED_RETENTION_KEY,
        cancelled_days,
        DEFAULT_CANCELLED_RETENTION_DAYS,
    )?;
    Retention::from_days(retention_days, cancelled_days).map_err(|error| match error {
        JobsError::OutOfRange {
            field: "retention_days",
            value,
        } => ServiceError::Configuration {
            key: RETENTION_KEY,
            value: value.to_string(),
        },
        JobsError::CancelledRetentionAboveRetention {
            cancelled,
            retention,
        } => ServiceError::Configuration {
            key: CANCELLED_RETENTION_KEY,
            value: format!("{cancelled} (must not exceed {RETENTION_KEY} = {retention})"),
        },
        JobsError::OutOfRange {
            field: "cancelled_retention_days",
            value,
        } => ServiceError::Configuration {
            key: CANCELLED_RETENTION_KEY,
            value: value.to_string(),
        },
        other => ServiceError::Domain(other),
    })
}

#[cfg(test)]
mod tests;
