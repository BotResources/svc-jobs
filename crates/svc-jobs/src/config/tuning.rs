use std::time::Duration;

use super::env::read;
use crate::error::ServiceError;
use crate::supervision::RestartPolicy;

const DEFAULT_CONSUMER_ACK_WAIT_SECONDS: u64 = 30;
const DEFAULT_CONSUMER_MAX_ACK_PENDING: i64 = 256;
const DEFAULT_CONSUMER_MAX_DELIVER: i64 = -1;
const DEFAULT_TASK_RESTART_INITIAL_BACKOFF_MILLISECONDS: u64 = 250;
const DEFAULT_TASK_RESTART_MAX_BACKOFF_SECONDS: u64 = 30;
const DEFAULT_TASK_STABILITY_SECONDS: u64 = 60;

#[derive(Clone, Copy)]
pub struct ConsumerTuning {
    pub ack_wait: Duration,
    pub max_ack_pending: i64,
    pub max_deliver: i64,
}

pub(super) fn consumer_tuning() -> Result<ConsumerTuning, ServiceError> {
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

pub(super) fn restart_policy() -> Result<RestartPolicy, ServiceError> {
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
        stability: Duration::from_secs(stability_seconds.max(1)),
    })
}
