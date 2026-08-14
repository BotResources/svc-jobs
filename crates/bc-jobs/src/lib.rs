//! `jobs` — the bounded context, as pure domain.
//!
//! Nothing in this crate does I/O. No database handle, no HTTP client, no
//! framework type crosses into it: a command takes the state it was rebuilt
//! from, returns the events it decided on, and that is the whole contract. That
//! purity is what makes the command tests the executable spec of this service —
//! they need no infrastructure to run, so there is never a reason not to write
//! one.
//!
//! The module tree below is generated whole rather than grown a directory at a
//! time. A `policies` module that only appears once someone writes the first
//! policy is a module whose placement gets re-decided by whoever gets there
//! first, and the doctrine already answered that question:
//!
//! - [`commands`] — one function per command, each pure, each returning a
//!   [`CommandResult`]. Split by aggregate then by capability, never one file
//!   per crate.
//! - [`domain`] — the model (aggregates and the value objects that make illegal
//!   states unrepresentable) and the actions (what an actor may do to an entity
//!   in its current state, which is what the edge turns into affordances).
//! - [`error`] — layer one of the three-layer error contract: invariant
//!   violations in the domain's own words.
//! - [`event`] — the typed domain facts, and the string constants they are
//!   stored under.
//! - [`policies`] — pure `event -> intent` functions. They produce data; the
//!   projector in `svc-jobs` is what executes it.
//! - [`ports`] — the traits through which the application reaches the outside.
//!   Segregated per concern, so no adapter inherits a method it must not have.

pub mod commands;
pub mod domain;
pub mod error;
pub mod event;
pub mod policies;
pub mod ports;

pub use commands::{CommandResult, CommandWarning};
pub use error::JobsError;
