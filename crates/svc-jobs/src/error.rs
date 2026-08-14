use thiserror::Error;

/// Layer two of the three-layer error contract: the application's categories.
///
/// The domain speaks in invariants (`bc_jobs::JobsError`), the edge
/// speaks in stable codes a frontend can act on, and this enum is the hinge
/// between them. It converts, it never leaks: a `sqlx` error becomes
/// [`ServiceError::Infra`] here rather than travelling to the edge as a database
/// message that would tell a caller what the schema looks like.
#[derive(Debug, Error)]
pub enum ServiceError {
    /// The request was well-formed enough to parse but not to act on.
    #[error("invalid request: {0}")]
    Validation(String),

    /// A domain invariant refused. Transparent, because the domain already
    /// phrased it and re-wrapping the sentence would only blur where it came
    /// from.
    #[error(transparent)]
    Domain(#[from] bc_jobs::JobsError),

    /// Infrastructure failed — a pool, the bus, the store. Carries a string
    /// rather than the source error on purpose: this is the variant the edge is
    /// most likely to render, and it must never carry infrastructure detail out
    /// of the process.
    #[error("infrastructure failure: {0}")]
    Infra(String),
}
