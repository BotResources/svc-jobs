#![allow(dead_code)]

pub mod adversary;
pub mod broker;
pub mod call_notices;
pub mod calls;
pub mod catalog;
pub mod claim;
pub mod clock;
pub mod codes;
pub mod contention;
pub mod db;
pub mod delta;
pub mod directory;
pub mod dispatch;
pub mod docs;
pub mod events;
pub mod fixture;
pub mod gesture;
pub mod gql;
pub mod infra;
pub mod lineage;
pub mod metrics;
pub mod passports;
pub mod priority;
pub mod producer;
pub mod refusal;
pub mod restart;
pub mod runner;
pub mod runner_cancel;
pub mod runner_presence;
pub mod spawn;
pub mod stream;
pub mod subs;
pub mod surface;
pub mod tree;
pub mod tree_hold;
pub mod trigger_stream;
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
pub const MY_MAX_PRIORITY_CHANGED: &str = "jobsMyMaxPriorityChanged";
pub const USER_MAX_PRIORITIES_CHANGED: &str = "jobsUserMaxPrioritiesChanged";
