//! Priority: the order in which waiting Jobs leave the ledger.
//!
//! A Job requests a level; its effective level is derived once, at admission,
//! from that request and the cap of the user it runs for — or inherited from
//! its parent. A super user may force a level, above any cap. Priority orders
//! waiting Jobs only: it never interrupts a started Run.

pub mod basis;
pub mod job_priority;
pub mod level;
pub mod user_max;

pub use basis::PriorityBasis;
pub use job_priority::JobPriority;
pub use level::Priority;
pub use user_max::UserMaxPriority;
