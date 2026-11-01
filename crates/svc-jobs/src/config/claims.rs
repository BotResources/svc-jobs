use std::time::Duration;

use chrono::TimeDelta;

use super::env::read;
use crate::error::ServiceError;

const DEFAULT_CLAIM_RESERVATION_SECONDS: i64 = 60;
const DEFAULT_CLAIM_MAX_WAIT_SECONDS: u64 = 30;
const DEFAULT_CLAIM_MAX_HELD: usize = 1024;

#[derive(Clone, Copy, Debug)]
pub struct ClaimSettings {
    pub max_wait: Duration,
    pub reservation_delay: TimeDelta,
    pub max_held: usize,
}

impl ClaimSettings {
    pub(super) fn from_environment() -> Result<Self, ServiceError> {
        let reservation_seconds: i64 = read(
            "JOBS_CLAIM_RESERVATION_SECONDS",
            DEFAULT_CLAIM_RESERVATION_SECONDS,
        )?;
        let max_wait_seconds: u64 = read(
            "JOBS_CLAIM_MAX_WAIT_SECONDS",
            DEFAULT_CLAIM_MAX_WAIT_SECONDS,
        )?;
        let max_held: usize = read("JOBS_CLAIM_MAX_HELD", DEFAULT_CLAIM_MAX_HELD)?;
        if max_held == 0 {
            return Err(ServiceError::Configuration {
                key: "JOBS_CLAIM_MAX_HELD",
                value: max_held.to_string(),
            });
        }
        if reservation_seconds <= 0 {
            return Err(ServiceError::Configuration {
                key: "JOBS_CLAIM_RESERVATION_SECONDS",
                value: reservation_seconds.to_string(),
            });
        }
        if max_wait_seconds == 0 {
            return Err(ServiceError::Configuration {
                key: "JOBS_CLAIM_MAX_WAIT_SECONDS",
                value: max_wait_seconds.to_string(),
            });
        }
        if u64::try_from(reservation_seconds).unwrap_or(0) <= max_wait_seconds {
            return Err(ServiceError::Configuration {
                key: "JOBS_CLAIM_RESERVATION_SECONDS",
                value: format!(
                    "{reservation_seconds} (must exceed JOBS_CLAIM_MAX_WAIT_SECONDS = \
                     {max_wait_seconds})"
                ),
            });
        }
        Ok(Self {
            max_wait: Duration::from_secs(max_wait_seconds),
            reservation_delay: TimeDelta::seconds(reservation_seconds),
            max_held,
        })
    }
}
