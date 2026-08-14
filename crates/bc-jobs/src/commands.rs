//! The commands of the `jobs` context — one pure function each.
//!
//! A command reads the aggregate it was handed, decides, and returns the facts
//! it decided on. It never appends, never publishes, never touches a pool: the
//! handler in `svc-jobs` is what loads, appends, projects and publishes,
//! and keeping that split is what lets every invariant be tested without
//! infrastructure.
//!
//! Grow this into a directory as the aggregates arrive — `commands/jobs/`
//! with one file per capability (`lifecycle.rs`, `membership.rs`), not one file
//! holding every command in the context.

use shared_kernel::DomainEvent;

/// What a command returns: the facts it decided on, plus anything the caller
/// should know that did not stop it.
///
/// Both halves matter. A command that only returned events would have to choose
/// between failing on a soft problem and swallowing it — the warning list is
/// what lets it succeed and still say what was odd.
#[derive(Debug, Clone)]
pub struct CommandResult {
    pub events: Vec<DomainEvent>,
    pub warnings: Vec<CommandWarning>,
}

impl CommandResult {
    /// Wrap a list of events with no warnings.
    pub fn new(events: Vec<DomainEvent>) -> Self {
        Self {
            events,
            warnings: vec![],
        }
    }

    /// Wrap a single event with no warnings.
    pub fn from_event(event: DomainEvent) -> Self {
        Self {
            events: vec![event],
            warnings: vec![],
        }
    }

    /// Add a warning to this result.
    pub fn with_warning(mut self, warning: CommandWarning) -> Self {
        self.warnings.push(warning);
        self
    }
}

/// Non-fatal outcomes a command reports alongside its events.
///
/// Uninhabited until this context has one to report, which is deliberate: an
/// empty enum makes every `match` on it exhaustive for free, and the first real
/// variant is then a compile error at exactly the places that must handle it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandWarning {}
