mod claims;
mod env;
mod nats;
mod retention;
mod tuning;

use std::time::Duration;

use bc_jobs::domain::attempts::MaxAttempts;
use bc_jobs::domain::policy::{RetryPolicy, ServiceLimits};
use bc_jobs::domain::priority::Priority;
use chrono::TimeDelta;

use crate::error::ServiceError;
use crate::supervision::RestartPolicy;

pub use claims::ClaimSettings;
pub use nats::NatsCredentials;
pub use tuning::ConsumerTuning;

use env::{read, required};
use nats::nats_credentials;
use retention::retention_from_environment;
use tuning::{consumer_tuning, restart_policy};

const DEFAULT_PORT: u16 = 8006;
const DEFAULT_INACTIVITY_TIMEOUT_SECONDS: i64 = 86_400;
const DEFAULT_RUN_MAX_DURATION_SECONDS: i64 = 259_200;
const DEFAULT_RETRY_BASE_DELAY_SECONDS: i64 = 10;
const DEFAULT_MAX_ATTEMPTS_CEILING: u32 = 10;
const DEFAULT_PAYLOAD_INLINE_MAX_BYTES: usize = 524_288;
const DEFAULT_SAVED_STATE_MAX_BYTES: usize = bc_jobs::domain::policy::DEFAULT_SAVED_STATE_MAX_BYTES;
const DEFAULT_BACKSTOP_INTERVAL_SECONDS: u64 = 30;
const DEFAULT_DISPATCH_MINIMUM_WAKE_MILLISECONDS: u64 = 250;
const DEFAULT_MAX_ATTEMPTS: u32 = 3;
const DEFAULT_RETRY_FACTOR: u32 = 3;
const DEFAULT_RETRY_JITTER_BASIS_POINTS: i64 = 2_000;
const DEFAULT_RETRY_MAX_DELAY_SECONDS: i64 = 3_600;
const DEFAULT_LOG_PARTITION_HORIZON_WARNING_DAYS: i64 = 180;
const DEFAULT_RETIREMENT_QUIET_PERIOD_SECONDS: i64 = 86_400;
const DEFAULT_RUNNER_TYPE_IMPACT_INTERVAL_SECONDS: u64 = 60;
const DEFAULT_CATALOG_RECONCILE_INTERVAL_SECONDS: u64 = 30;
const DEFAULT_MAX_PRIORITY: i64 = 3;

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
    pub runner_type_impact_interval: Duration,
    pub catalog_reconcile_interval: Duration,
    pub consumer_tuning: ConsumerTuning,
    pub restart_policy: RestartPolicy,
    pub log_partition_horizon_warning: TimeDelta,
    pub claims: ClaimSettings,
    pub scope_declaration_enabled: bool,
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
        let payload_inline_max_bytes: usize = read(
            "JOBS_PAYLOAD_INLINE_MAX_BYTES",
            DEFAULT_PAYLOAD_INLINE_MAX_BYTES,
        )?;
        let saved_state_max_bytes: usize =
            read("JOBS_SAVED_STATE_MAX_BYTES", DEFAULT_SAVED_STATE_MAX_BYTES)?;
        let backstop_interval_seconds: u64 = read(
            "JOBS_BACKSTOP_INTERVAL_SECONDS",
            DEFAULT_BACKSTOP_INTERVAL_SECONDS,
        )?;
        let dispatch_minimum_wake_milliseconds: u64 = read(
            "JOBS_DISPATCH_MINIMUM_WAKE_MILLISECONDS",
            DEFAULT_DISPATCH_MINIMUM_WAKE_MILLISECONDS,
        )?;
        let retirement_quiet_period_seconds: i64 = read(
            "JOBS_RETIREMENT_QUIET_PERIOD_SECONDS",
            DEFAULT_RETIREMENT_QUIET_PERIOD_SECONDS,
        )?;
        let runner_type_impact_interval_seconds: u64 = read(
            "JOBS_RUNNER_TYPE_IMPACT_INTERVAL_SECONDS",
            DEFAULT_RUNNER_TYPE_IMPACT_INTERVAL_SECONDS,
        )?;
        let catalog_reconcile_interval_seconds: u64 = read(
            "JOBS_CATALOG_RECONCILE_INTERVAL_SECONDS",
            DEFAULT_CATALOG_RECONCILE_INTERVAL_SECONDS,
        )?;

        let default_max_priority: i64 = read("JOBS_DEFAULT_MAX_PRIORITY", DEFAULT_MAX_PRIORITY)?;

        let ceiling = MaxAttempts::new(max_attempts_ceiling)?;
        let limits = ServiceLimits::new(
            ceiling,
            MaxAttempts::new(DEFAULT_MAX_ATTEMPTS.min(max_attempts_ceiling))?,
            TimeDelta::seconds(run_max_duration_seconds),
            TimeDelta::seconds(inactivity_timeout_seconds),
            TimeDelta::seconds(retirement_quiet_period_seconds),
        )?
        .with_default_max_priority(Priority::new(default_max_priority)?)
        .with_inline_payload_max_bytes(payload_inline_max_bytes)
        .with_saved_state_max_bytes(saved_state_max_bytes)
        .with_retention(retention_from_environment()?);
        bc_jobs::domain::fleet::run_duration::MaxRunDuration::from_seconds(
            run_max_duration_seconds,
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
            runner_type_impact_interval: Duration::from_secs(
                runner_type_impact_interval_seconds.max(1),
            ),
            catalog_reconcile_interval: Duration::from_secs(
                catalog_reconcile_interval_seconds.max(1),
            ),
            consumer_tuning: consumer_tuning()?,
            restart_policy: restart_policy()?,
            log_partition_horizon_warning: TimeDelta::days(read(
                "JOBS_LOG_PARTITION_HORIZON_WARNING_DAYS",
                DEFAULT_LOG_PARTITION_HORIZON_WARNING_DAYS,
            )?),
            claims: ClaimSettings::from_environment()?,
            scope_declaration_enabled: read("SCOPE_DECLARATION_ENABLED", true)?,
        })
    }
}
