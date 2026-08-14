//! The domain facts of the `jobs` context.
//!
//! Two things live here and they are not the same thing: the `&str` constants an
//! event is *stored* under, and the typed payload each one carries. The constant
//! is a wire value — once an event of that type is in the store, renaming the
//! constant renames nothing already written, so it is frozen from the first
//! append.
//!
//! Events are granular and named for the fact, in the past tense. If a
//! subscriber has to run a query to find out what actually changed, the event is
//! too coarse and needs splitting rather than a richer payload. And an event
//! named `SomethingRequested` is a command wearing the wrong clothes — a demand
//! aimed at another context belongs on that context's command subject, not in
//! this file.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// The aggregate every event below belongs to, stored verbatim beside each
/// recorded event. In an EDA-FULL major that is `domain_events.aggregate_type`,
/// which the load-by-aggregate query filters on; a SOFT major records it on
/// whatever fact table it keeps.
pub const AGGREGATE_TYPE: &str = "Jobs";

/// The placeholder first fact: the aggregate came into existence.
///
/// Kept so the crate has a working example of the constant-plus-payload pair,
/// and so `svc-jobs` has something to project. Rename it to the real
/// first fact of this context before anything is appended under it.
pub const JOBS_CREATED: &str = "JobsCreated";

/// The payload of [`JOBS_CREATED`].
///
/// The id is the creator-supplied UUIDv7 the command was called with — this
/// service never mints one.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobsCreatedPayload {
    pub jobs_id: Uuid,
}
