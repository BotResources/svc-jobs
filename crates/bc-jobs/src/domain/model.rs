//! The aggregates and value objects of the `jobs` context.
//!
//! Two rules govern everything that lands here, and both push validation out of
//! the commands and into the types:
//!
//! - **No raw primitives for domain concepts.** A name is a `JobsName` that
//!   rejects the empty string at construction, not a `String` every caller has
//!   to remember to trim. Illegal states become unrepresentable rather than
//!   merely unlikely.
//! - **Invariants are checked on hydration.** An aggregate rebuilt from its
//!   event stream validates itself on the way up, so a corrupt stream fails
//!   loudly at load instead of quietly at the next command.
//!
//! Grow this into a directory — one file per aggregate or value object — rather
//! than one long module.
