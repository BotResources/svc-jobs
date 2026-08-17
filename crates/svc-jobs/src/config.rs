use std::time::Duration;

use bc_jobs::domain::attempts::MaxAttempts;
use bc_jobs::domain::policy::{RetryPolicy, ServiceLimits};
use chrono::TimeDelta;

use crate::error::ServiceError;
use crate::supervision::RestartPolicy;

const DEFAULT_PORT: u16 = 8006;
const DEFAULT_INACTIVITY_TIMEOUT_SECONDS: i64 = 86_400;
const DEFAULT_RUN_MAX_DURATION_SECONDS: i64 = 259_200;
const DEFAULT_RETRY_BASE_DELAY_SECONDS: i64 = 10;
const DEFAULT_MAX_ATTEMPTS_CEILING: u32 = 10;
const DEFAULT_BACKSTOP_INTERVAL_SECONDS: u64 = 30;
const DEFAULT_DISPATCH_MINIMUM_WAKE_MILLISECONDS: u64 = 250;
const DEFAULT_MAX_ATTEMPTS: u32 = 3;
const DEFAULT_RETRY_FACTOR: u32 = 3;
const DEFAULT_RETRY_JITTER_BASIS_POINTS: i64 = 2_000;
const DEFAULT_RETRY_MAX_DELAY_SECONDS: i64 = 3_600;
const DEFAULT_CONSUMER_ACK_WAIT_SECONDS: u64 = 30;
const DEFAULT_CONSUMER_MAX_ACK_PENDING: i64 = 256;
const DEFAULT_CONSUMER_MAX_DELIVER: i64 = -1;
const DEFAULT_TASK_RESTART_INITIAL_BACKOFF_MILLISECONDS: u64 = 250;
const DEFAULT_TASK_RESTART_MAX_BACKOFF_SECONDS: u64 = 30;
const DEFAULT_TASK_RESTART_BUDGET: u32 = 10;
const DEFAULT_TASK_STABILITY_SECONDS: u64 = 60;
const DEFAULT_LOG_PARTITION_HORIZON_WARNING_DAYS: i64 = 180;

pub struct Settings {
    pub port: u16,
    pub database_url: String,
    pub nats_url: String,
    pub nats_credentials: Option<NatsCredentials>,
    pub app_password: Option<String>,
    pub limits: ServiceLimits,
    pub retry_policy: RetryPolicy,
    pub backstop_interval: Duration,
    pub dispatch_minimum_wake: Duration,
    pub consumer_tuning: ConsumerTuning,
    pub restart_policy: RestartPolicy,
    pub log_partition_horizon_warning: TimeDelta,
}

#[derive(Clone, Copy)]
pub struct ConsumerTuning {
    pub ack_wait: Duration,
    pub max_ack_pending: i64,
    pub max_deliver: i64,
}

pub struct NatsCredentials {
    pub user: String,
    pub password: String,
}

fn declared(key: &'static str) -> Option<String> {
    std::env::var(key).ok().filter(|value| !value.is_empty())
}

fn nats_credentials() -> Result<Option<NatsCredentials>, ServiceError> {
    match (declared("NATS_USER"), declared("NATS_PASSWORD")) {
        (None, None) => Ok(None),
        (Some(user), Some(password)) => Ok(Some(NatsCredentials { user, password })),
        (Some(_), None) => Err(ServiceError::MissingConfiguration {
            key: "NATS_PASSWORD",
        }),
        (None, Some(_)) => Err(ServiceError::MissingConfiguration { key: "NATS_USER" }),
    }
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
        let retry_max_delay_seconds = read(
            "JOBS_RETRY_MAX_DELAY_SECONDS",
            DEFAULT_RETRY_MAX_DELAY_SECONDS,
        )?;
        let retry_factor: u32 = read("JOBS_RETRY_FACTOR", DEFAULT_RETRY_FACTOR)?;
        let retry_jitter_basis_points: i64 = read(
            "JOBS_RETRY_JITTER_BASIS_POINTS",
            DEFAULT_RETRY_JITTER_BASIS_POINTS,
        )?;
        let max_attempts_ceiling: u32 =
            read("JOBS_MAX_ATTEMPTS_CEILING", DEFAULT_MAX_ATTEMPTS_CEILING)?;
        let backstop_interval_seconds: u64 = read(
            "JOBS_BACKSTOP_INTERVAL_SECONDS",
            DEFAULT_BACKSTOP_INTERVAL_SECONDS,
        )?;
        let dispatch_minimum_wake_milliseconds: u64 = read(
            "JOBS_DISPATCH_MINIMUM_WAKE_MILLISECONDS",
            DEFAULT_DISPATCH_MINIMUM_WAKE_MILLISECONDS,
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
            retry_factor,
            TimeDelta::seconds(retry_max_delay_seconds),
            retry_jitter_basis_points,
        )?;

        Ok(Self {
            port: read("PORT", DEFAULT_PORT)?,
            database_url: required("DATABASE_URL")?,
            nats_url: required("NATS_URL")?,
            nats_credentials: nats_credentials()?,
            app_password: std::env::var("JOBS_APP_PASSWORD").ok(),
            limits,
            retry_policy,
            backstop_interval: Duration::from_secs(backstop_interval_seconds.max(1)),
            dispatch_minimum_wake: Duration::from_millis(dispatch_minimum_wake_milliseconds.max(1)),
            consumer_tuning: consumer_tuning()?,
            restart_policy: restart_policy()?,
            log_partition_horizon_warning: TimeDelta::days(read(
                "JOBS_LOG_PARTITION_HORIZON_WARNING_DAYS",
                DEFAULT_LOG_PARTITION_HORIZON_WARNING_DAYS,
            )?),
        })
    }
}

fn consumer_tuning() -> Result<ConsumerTuning, ServiceError> {
    let ack_wait_seconds: u64 = read(
        "JOBS_CONSUMER_ACK_WAIT_SECONDS",
        DEFAULT_CONSUMER_ACK_WAIT_SECONDS,
    )?;
    Ok(ConsumerTuning {
        ack_wait: Duration::from_secs(ack_wait_seconds.max(1)),
        max_ack_pending: read(
            "JOBS_CONSUMER_MAX_ACK_PENDING",
            DEFAULT_CONSUMER_MAX_ACK_PENDING,
        )?,
        max_deliver: read("JOBS_CONSUMER_MAX_DELIVER", DEFAULT_CONSUMER_MAX_DELIVER)?,
    })
}

fn restart_policy() -> Result<RestartPolicy, ServiceError> {
    let initial_backoff_milliseconds: u64 = read(
        "JOBS_TASK_RESTART_INITIAL_BACKOFF_MILLISECONDS",
        DEFAULT_TASK_RESTART_INITIAL_BACKOFF_MILLISECONDS,
    )?;
    let max_backoff_seconds: u64 = read(
        "JOBS_TASK_RESTART_MAX_BACKOFF_SECONDS",
        DEFAULT_TASK_RESTART_MAX_BACKOFF_SECONDS,
    )?;
    let stability_seconds: u64 = read(
        "JOBS_TASK_STABILITY_SECONDS",
        DEFAULT_TASK_STABILITY_SECONDS,
    )?;
    Ok(RestartPolicy {
        initial_backoff: Duration::from_millis(initial_backoff_milliseconds.max(1)),
        max_backoff: Duration::from_secs(max_backoff_seconds.max(1)),
        budget: read("JOBS_TASK_RESTART_BUDGET", DEFAULT_TASK_RESTART_BUDGET)?,
        stability: Duration::from_secs(stability_seconds.max(1)),
    })
}
