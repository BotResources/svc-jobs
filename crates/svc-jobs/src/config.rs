use std::time::Duration;

use bc_jobs::domain::attempts::MaxAttempts;
use bc_jobs::domain::policy::{RetryPolicy, ServiceLimits};
use chrono::TimeDelta;

use crate::error::ServiceError;

const DEFAULT_PORT: u16 = 8006;
const DEFAULT_INACTIVITY_TIMEOUT_SECONDS: i64 = 86_400;
const DEFAULT_RUN_MAX_DURATION_SECONDS: i64 = 259_200;
const DEFAULT_RETRY_BASE_DELAY_SECONDS: i64 = 10;
const DEFAULT_MAX_ATTEMPTS_CEILING: u32 = 10;
const DEFAULT_BACKSTOP_INTERVAL_SECONDS: u64 = 30;
const DEFAULT_MAX_ATTEMPTS: u32 = 3;
const RETRY_FACTOR: u32 = 3;
const RETRY_JITTER_BASIS_POINTS: i64 = 2_000;
const RETRY_MAX_DELAY_FACTOR: i64 = 360;

pub struct Settings {
    pub port: u16,
    pub database_url: String,
    pub nats_url: String,
    pub app_password: Option<String>,
    pub limits: ServiceLimits,
    pub retry_policy: RetryPolicy,
    pub backstop_interval: Duration,
}

fn read<T: std::str::FromStr>(key: &'static str, fallback: T) -> Result<T, ServiceError> {
    match std::env::var(key) {
        Err(_) => Ok(fallback),
        Ok(raw) => raw
            .parse()
            .map_err(|_| ServiceError::Configuration { key, value: raw }),
    }
}

fn required(key: &'static str) -> Result<String, ServiceError> {
    std::env::var(key).map_err(|_| ServiceError::MissingConfiguration { key })
}

impl Settings {
    pub fn from_environment() -> Result<Self, ServiceError> {
        let inactivity_timeout_seconds = read(
            "JOBS_INACTIVITY_TIMEOUT_SECONDS",
            DEFAULT_INACTIVITY_TIMEOUT_SECONDS,
        )?;
        let run_max_duration_seconds = read(
            "JOBS_RUN_MAX_DURATION_SECONDS",
            DEFAULT_RUN_MAX_DURATION_SECONDS,
        )?;
        let retry_base_delay_seconds = read(
            "JOBS_RETRY_BASE_DELAY_SECONDS",
            DEFAULT_RETRY_BASE_DELAY_SECONDS,
        )?;
        let max_attempts_ceiling: u32 =
            read("JOBS_MAX_ATTEMPTS_CEILING", DEFAULT_MAX_ATTEMPTS_CEILING)?;
        let backstop_interval_seconds: u64 = read(
            "JOBS_BACKSTOP_INTERVAL_SECONDS",
            DEFAULT_BACKSTOP_INTERVAL_SECONDS,
        )?;

        let ceiling = MaxAttempts::new(max_attempts_ceiling)?;
        let limits = ServiceLimits::new(
            ceiling,
            MaxAttempts::new(DEFAULT_MAX_ATTEMPTS.min(max_attempts_ceiling))?,
            TimeDelta::seconds(run_max_duration_seconds),
            TimeDelta::seconds(inactivity_timeout_seconds),
        )?;
        let retry_policy = RetryPolicy::new(
            TimeDelta::seconds(retry_base_delay_seconds),
            RETRY_FACTOR,
            TimeDelta::seconds(retry_base_delay_seconds.saturating_mul(RETRY_MAX_DELAY_FACTOR)),
            RETRY_JITTER_BASIS_POINTS,
        )?;

        Ok(Self {
            port: read("PORT", DEFAULT_PORT)?,
            database_url: required("DATABASE_URL")?,
            nats_url: required("NATS_URL")?,
            app_password: std::env::var("JOBS_APP_PASSWORD").ok(),
            limits,
            retry_policy,
            backstop_interval: Duration::from_secs(backstop_interval_seconds.max(1)),
        })
    }
}
