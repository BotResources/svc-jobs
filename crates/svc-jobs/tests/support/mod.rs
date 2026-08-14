#![allow(dead_code)]

pub mod docs;
pub mod events;
pub mod fixture;
pub mod gql;
pub mod producer;
pub mod runner;
pub mod stream;
pub mod wire;

use std::time::Duration;

pub const SHORT: Duration = Duration::from_secs(5);
pub const LONG: Duration = Duration::from_secs(20);
pub const QUIET: Duration = Duration::from_secs(3);
