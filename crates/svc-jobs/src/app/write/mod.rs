//! The write paths of the service: Job changes, RunnerType changes and
//! published rejections, each checking under lock that what it decided on
//! still holds.

mod fleet;
mod job_changes;
mod premise;
mod rederive;
mod rejection;

pub use fleet::{FleetChange, commit_fleet, commit_fleet_events};
pub use job_changes::{
    JobChange, WriteContext, commit_job_changes, commit_job_changes_in, record_user_events_in,
};
pub use premise::PriorityPremise;
pub use rejection::{CreationRefusal, publish_command_rejection, publish_rejection};
