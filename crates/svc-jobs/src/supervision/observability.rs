//! What the supervisor publishes about the background tasks it keeps alive.
//!
//! A task that lost the bus is retried for as long as the process lives, and the
//! pod stays NOT READY meanwhile. Readiness takes the pod out of rotation; it does
//! not tell anyone why, nor for how long. The two series below are what an alert
//! is written on: a task down for minutes, a task restarting again and again.
//! Labels are task names — a fixed set of static strings, never a wire value.

use metrics::{counter, describe_counter, describe_gauge, gauge};

pub const TASK_UP: &str = "jobs_background_task_up";
pub const TASK_RESTARTS_TOTAL: &str = "jobs_background_task_restarts_total";

const TASK: &str = "task";

/// Called once, when the supervisor is built.
pub fn describe() {
    describe_gauge!(
        TASK_UP,
        "1 while a supervised background task is bound to its source of work, 0 from the moment \
         an attempt ends until it binds again. The pod is ready only while every task reads 1"
    );
    describe_counter!(
        TASK_RESTARTS_TOTAL,
        "Restarts of a supervised background task after it ended. A task is restarted for as \
         long as the process lives; a series that keeps rising means its dependency is still out"
    );
}

/// A task enters supervision: both series exist before anything has failed, so
/// an alert on either is armed from boot.
pub fn spawned(task: &'static str) {
    gauge!(TASK_UP, TASK => task).set(0.0);
    counter!(TASK_RESTARTS_TOTAL, TASK => task).absolute(0);
}

pub fn bound(task: &'static str) {
    gauge!(TASK_UP, TASK => task).set(1.0);
}

pub fn lost(task: &'static str) {
    gauge!(TASK_UP, TASK => task).set(0.0);
}

pub fn restarted(task: &'static str) {
    counter!(TASK_RESTARTS_TOTAL, TASK => task).increment(1);
}
