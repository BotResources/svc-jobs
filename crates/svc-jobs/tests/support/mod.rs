#![allow(dead_code)]

pub mod adversary;
pub mod catalog;
pub mod clock;
pub mod codes;
pub mod contention;
pub mod db;
pub mod delta;
pub mod docs;
pub mod events;
pub mod fixture;
pub mod gql;
pub mod infra;
pub mod producer;
pub mod runner;
pub mod stream;
pub mod subs;
pub mod views;
pub mod wire;

use std::time::Duration;

pub const SHORT: Duration = Duration::from_secs(5);
pub const LONG: Duration = Duration::from_secs(20);
pub const QUIET: Duration = Duration::from_secs(3);

pub const JOB_CHANGED: &str = "jobsJobChanged";
pub const JOBS_CHANGED: &str = "jobsChanged";
pub const LOG_TAIL: &str = "jobsJobLogTail";
pub const FLEET_CHANGED: &str = "jobsFleetChanged";
